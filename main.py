import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from astrbot.api.all import Star, Context, register, command, AstrMessageEvent
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from astrbot.api.event import MessageChain

# ==================== 原版核心解析逻辑 ====================

def fetch_page(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text

def parse_current_slot(soup: BeautifulSoup) -> dict[str, Any]:
    time_items = soup.select(".time-list li")
    if not time_items:
        raise ValueError("Failed to find time slots in merchant page")

    for index, element in enumerate(time_items, start=1):
        if "on" not in element.get("class", []):
            continue
        values = [em.get_text(strip=True) for em in element.select("em")]
        start = values[0] if len(values) > 0 else ""
        end = values[1] if len(values) > 1 else ""
        return {
            "index": int(element.get("data-index", index)),
            "label": f"{start}-{end}" if start and end else f"slot-{index}",
            "start": start,
            "end": end,
        }

    first = time_items[0]
    values = [em.get_text(strip=True) for em in first.select("em")]
    return {
        "index": int(first.get("data-index", 1)),
        "label": f"{values[0]}-{values[1]}" if len(values) >= 2 else "slot-1",
        "start": values[0] if len(values) >= 1 else "",
        "end": values[1] if len(values) >= 2 else "",
    }

def parse_items(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    slot = parse_current_slot(soup)
    raw_items = soup.select(f".shop-list li.show_{slot['index']}")

    items: list[dict[str, Any]] = []
    for slot_position, element in enumerate(raw_items, start=1):
        if "show_none_tip" in element.get("class", []):
            continue

        name_el = element.select_one(".shop_name")
        price_el = element.select_one(".shop_price")
        if not name_el or not price_el:
            continue

        name = name_el.get_text(strip=True)
        price_text = price_el.get_text(strip=True).replace("价格：", "").strip()
        
        limit_em = element.select_one(".gitem em")
        limit_text = limit_em.get_text(strip=True) if limit_em else ""

        items.append({
            "slot": slot_position,
            "name": name,
            "price_text": price_text,
            "limit_text": limit_text,
        })

    if not items:
        raise ValueError(f"Failed to parse any items for {slot['label']}")
    return {"slot": slot, "items": items}

def comparable_payload(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "slot": data["slot"],
        "items": [
            {
                "slot": item["slot"],
                "name": item["name"],
                "price_text": item["price_text"],
                "limit_text": item["limit_text"],
            }
            for item in data["items"]
        ],
    }

def has_changed(previous: dict[str, Any] | None, current: dict[str, Any]) -> bool:
    if previous is None:
        return True
    return comparable_payload(previous) != comparable_payload(current)


# ==================== AstrBot 插件实体 ====================

@register("astrbot_plugin_rock_merchant_notify", "AstrBot", "远行商人监控插件", "1.3.0", "支持不同群聊独立配置推送模式及艾特规则")
class MerchantNotifyPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        
        self.data_dir = Path(get_astrbot_data_path()) / "plugin_data" / self.name
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "latest.json"
        self.history_path = self.data_dir / "history.jsonl"
        self.subscribers_path = self.data_dir / "subscribers.json" 
        self.timezone = "Asia/Shanghai"
        
        self.subscribers = self._load_subscribers()
        
        # 新增：用于控制单次时间窗口内抓取频次的状态变量
        self.current_active_slot = None
        self.attempt_count = 0
        self.slot_success = False
        self.last_fetch_result = None  # 保存上一次抓取结果用于比较
        
        # 启动后台定时轮询任务
        self._polling_task = asyncio.create_task(self.poll_loop())

    def _load_subscribers(self) -> dict[str, dict]:
        if self.subscribers_path.exists():
            try:
                data = json.loads(self.subscribers_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    return {umo: {"push_mode": 1, "mention_everyone": 0} for umo in data}
                return data
            except Exception as e:
                logging.error(f"[{self.name}] 读取订阅列表失败: {e}")
        return {}

    def _save_subscribers(self):
        self.subscribers_path.write_text(json.dumps(self.subscribers, ensure_ascii=False), encoding="utf-8")

    def get_watch_items(self) -> list[str]:
        raw = self.config.get("watch_items", "国王球,棱镜球,炫彩精灵蛋,祝福吊坠")
        return [i.strip() for i in raw.split(",") if i.strip()]

    def get_active_slot(self, current_time: datetime) -> str | None:
        """获取当前时间是否在任意配置的 [+60分钟] 窗口内，并返回对应的槽位时间字符串"""
        current_minutes = current_time.hour * 60 + current_time.minute
        window = self.config.get("refresh_window_minutes", 60)
        times_str = self.config.get("refresh_times", "08:01,12:01,16:01,20:01")
        
        for item in [t.strip() for t in times_str.split(",") if t.strip()]:
            hour_str, minute_str = item.split(":", 1)
            scheduled_minutes = int(hour_str) * 60 + int(minute_str)
            
            diff = current_minutes - scheduled_minutes
            # 兼容极端的跨天配置 (比如配置 23:30，当前是 00:10)
            if diff < 0 and current_minutes < window:
                diff += 1440
                
            # 从 ±60 改为严格的后方 +60
            if 0 <= diff <= window:
                return item
        return None

    async def poll_loop(self):
        """后台轮询任务，支持完成打断与次数限制熔断"""
        while True:
            try:
                current_time = datetime.now(ZoneInfo(self.timezone))
                active_slot = self.get_active_slot(current_time)
                
                if not active_slot:
                    # 不在任何窗口期，清理状态并保持沉睡
                    self.current_active_slot = None
                    self.attempt_count = 0
                    self.slot_success = False
                    self.last_fetch_result = None
                else:
                    # 进入了一个新的时间窗口
                    if active_slot != self.current_active_slot:
                        self.current_active_slot = active_slot
                        self.attempt_count = 0
                        self.slot_success = False
                        self.last_fetch_result = None
                    
                    # 只有在未获取到新数据，且尝试次数没超限时，才执行抓取
                    if not self.slot_success and self.attempt_count < 5:
                        self.attempt_count += 1
                        logging.info(f"[{self.name}] 正在执行 {active_slot} 窗口期的第 {self.attempt_count}/5 次抓取检测...")
                        
                        # 执行抓取但不立即推送，仅获取数据
                        current_payload, _ = await self.execute_fetch_only()
                        
                        # 第一次抓取：保存结果，继续下一次
                        if self.attempt_count == 1:
                            self.last_fetch_result = current_payload
                            logging.info(f"[{self.name}] {active_slot} 第1次抓取完成，等待下次抓取比对...")
                        else:
                            # 第二次及以后：与上一次结果比较
                            if comparable_payload(self.last_fetch_result) == comparable_payload(current_payload):
                                # 两次结果一致，使用上一次保存的状态判断是否需要推送
                                await self.process_and_push(current_payload, send_alert=True)
                                self.slot_success = True
                                logging.info(f"[{self.name}] {active_slot} 连续两次抓取结果一致！推送完毕，本窗口期不再继续抓取。")
                            else:
                                # 结果不一致，更新缓存继续抓取
                                self.last_fetch_result = current_payload
                                logging.info(f"[{self.name}] {active_slot} 第 {self.attempt_count} 次抓取结果与上次不一致，继续比对...")
                            
                            # 达到最大次数兜底
                            if self.attempt_count >= 5 and not self.slot_success:
                                # 使用最后一次结果推送
                                await self.process_and_push(current_payload, send_alert=True)
                                self.slot_success = True
                                logging.info(f"[{self.name}] {active_slot} 已达到最大检测次数限制 5，使用最后一次结果推送，本窗口期不再继续抓取。")

            except Exception as e:
                logging.error(f"[{self.name}] Polling error: {e}")
            
            # 基础周期改为2分钟
            await asyncio.sleep(120)

    async def execute_fetch_only(self) -> tuple[dict[str, Any], bool]:
        """仅执行抓取流程，返回解析后的payload和是否变更标志（不推送）"""
        current_time = datetime.now(ZoneInfo(self.timezone))
        
        url = self.config.get("merchant_url", "")
        if not url:
            raise ValueError("配置错误：未提供 merchant_url")

        html = await asyncio.to_thread(fetch_page, url)
        parsed = await asyncio.to_thread(parse_items, html)
        
        payload = {
            "fetched_at": current_time.isoformat(),
            "slot": parsed["slot"],
            "items": parsed["items"],
        }
        
        # 读取上次保存的状态用于比对
        previous = None
        if self.state_path.exists():
            try:
                previous = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.error(f"[{self.name}] 读取历史状态失败: {e}")

        changed = has_changed(previous, payload)
        
        return payload, changed

    async def process_and_push(self, payload: dict[str, Any], send_alert: bool) -> str:
        """处理抓取结果，保存状态并推送通知"""
        current_time = datetime.now(ZoneInfo(self.timezone))
        
        # 读取上次保存的状态
        previous = None
        if self.state_path.exists():
            try:
                previous = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.error(f"[{self.name}] 读取历史状态失败: {e}")

        changed = has_changed(previous, payload)
        
        # 判断时间兜底
        if not changed and previous and "fetched_at" in previous:
            try:
                display_time = datetime.fromisoformat(previous["fetched_at"])
            except Exception:
                display_time = current_time
        else:
            display_time = current_time
        
        watch_items = self.get_watch_items()
        matched = [item["name"] for item in payload["items"] if item["name"] in watch_items]
        
        slot_label = payload["slot"]["label"]
        title = f"远行商人已刷新 {slot_label}" if not matched else f"远行商人命中关注商品 {slot_label}"
        
        lines = []
        if matched:
            lines.append(f"🎯 关注商品：{', '.join(matched)}\n")
        for item in payload["items"]:
            price = item["price_text"] or "未知价格"
            limit = item["limit_text"] or "限购未知"
            lines.append(f"{item['slot']}. {item['name']} | {price} | {limit}")
            
        message = "\n".join(lines)
        time_str = display_time.strftime("%Y-%m-%d %H:%M")
        full_text = f"【{title}】\n{message}\n\n🕒 检查时间：{time_str}"

        # 状态发生变化时保存数据
        if changed:
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            
            history_lines = []
            if self.history_path.exists():
                try:
                    with self.history_path.open("r", encoding="utf-8") as fp:
                        history_lines = fp.readlines()
                except Exception as e:
                    logging.error(f"[{self.name}] 读取历史记录失败: {e}")
            
            history_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")
            history_lines = history_lines[-20:]
            
            try:
                with self.history_path.open("w", encoding="utf-8") as fp:
                    fp.writelines(history_lines)
            except Exception as e:
                logging.error(f"[{self.name}] 写入历史记录失败: {e}")
            
            # 分发推送
            if send_alert and self.subscribers:
                logging.info(f"[{self.name}] 检测到数据更新，开始处理分群过滤推送...")
                for umo, settings in self.subscribers.items():
                    push_mode = settings.get("push_mode", 1)
                    mention_everyone = settings.get("mention_everyone", 0)
                    
                    should_push = False
                    if push_mode == 0:     
                        should_push = True
                    elif push_mode == 1:   
                        if matched:
                            should_push = True
                    
                    if should_push:
                        try:
                            chain = MessageChain()
                            if mention_everyone == 1:
                                chain.at_all()
                            chain.message(full_text)
                            # 使用 asyncio.create_task 避免单一群聊发送卡死阻塞总流程
                            asyncio.create_task(self.context.send_message(umo, chain))
                        except Exception as e:
                            logging.error(f"[{self.name}] 向 {umo} 推送失败: {e}")
        
        return full_text

    async def execute_check(self, send_alert: bool) -> tuple[str, bool]:
        """执行抓取流程并推送，返回值 -> (拼接好的消息文本, 数据是否变更)"""
        current_time = datetime.now(ZoneInfo(self.timezone))
        
        url = self.config.get("merchant_url", "")
        if not url:
            return "配置错误：未提供 merchant_url", False

        html = await asyncio.to_thread(fetch_page, url)
        parsed = await asyncio.to_thread(parse_items, html)
        
        payload = {
            "fetched_at": current_time.isoformat(),
            "slot": parsed["slot"],
            "items": parsed["items"],
        }
        
        previous = None
        if self.state_path.exists():
            try:
                previous = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.error(f"[{self.name}] 读取历史状态失败: {e}")

        changed = has_changed(previous, payload)
        
        # 判断时间兜底
        if not changed and previous and "fetched_at" in previous:
            try:
                display_time = datetime.fromisoformat(previous["fetched_at"])
            except Exception:
                display_time = current_time
        else:
            display_time = current_time
        
        watch_items = self.get_watch_items()
        matched = [item["name"] for item in payload["items"] if item["name"] in watch_items]
        
        slot_label = payload["slot"]["label"]
        title = f"远行商人已刷新 {slot_label}" if not matched else f"远行商人命中关注商品 {slot_label}"
        
        lines = []
        if matched:
            lines.append(f"🎯 关注商品：{', '.join(matched)}\n")
        for item in payload["items"]:
            price = item["price_text"] or "未知价格"
            limit = item["limit_text"] or "限购未知"
            lines.append(f"{item['slot']}. {item['name']} | {price} | {limit}")
            
        message = "\n".join(lines)
        time_str = display_time.strftime("%Y-%m-%d %H:%M")
        full_text = f"【{title}】\n{message}\n\n🕒 检查时间：{time_str}"

        # 状态发生变化时保存数据
        if changed:
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            
            history_lines = []
            if self.history_path.exists():
                try:
                    with self.history_path.open("r", encoding="utf-8") as fp:
                        history_lines = fp.readlines()
                except Exception as e:
                    logging.error(f"[{self.name}] 读取历史记录失败: {e}")
            
            history_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")
            history_lines = history_lines[-20:]
            
            try:
                with self.history_path.open("w", encoding="utf-8") as fp:
                    fp.writelines(history_lines)
            except Exception as e:
                logging.error(f"[{self.name}] 写入历史记录失败: {e}")
            
            # 分发推送
            if send_alert and self.subscribers:
                logging.info(f"[{self.name}] 检测到数据更新，开始处理分群过滤推送...")
                for umo, settings in self.subscribers.items():
                    push_mode = settings.get("push_mode", 1)
                    mention_everyone = settings.get("mention_everyone", 0)
                    
                    should_push = False
                    if push_mode == 0:     
                        should_push = True
                    elif push_mode == 1:   
                        if matched:
                            should_push = True
                    
                    if should_push:
                        try:
                            chain = MessageChain()
                            if mention_everyone == 1:
                                chain.at_all()
                            chain.message(full_text)
                            # 使用 asyncio.create_task 避免单一群聊发送卡死阻塞总流程
                            asyncio.create_task(self.context.send_message(umo, chain))
                        except Exception as e:
                            logging.error(f"[{self.name}] 向 {umo} 推送失败: {e}")
        
        return full_text, changed

    @command("订阅商人")
    async def subscribe(self, event: AstrMessageEvent):
        """订阅远行商人刷新自动推送"""
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            self.subscribers[umo] = {
                "push_mode": 1,
                "mention_everyone": 0
            }
            self._save_subscribers()
            yield event.plain_result(
                "✅ 订阅成功！远行商人刷新时本窗口将自动收到推送。\n\n"
                "💡 当前群聊初始默认规则：\n"
                "- 推送模式：【1】只匹配到商品推送\n"
                "- 艾特模式：【0】不@全员\n\n"
                "⚙️ 群内可用管理指令：\n"
                "- /设置商人推送 [0/1/2]\n"
                "- /设置商人艾特 [0/1]"
            )
        else:
            yield event.plain_result("⚠️ 当前位置已经订阅过了，无需重复订阅。")

    @command("取消订阅商人")
    async def unsubscribe(self, event: AstrMessageEvent):
        """取消订阅远行商人刷新推送"""
        umo = event.unified_msg_origin
        if umo in self.subscribers:
            del self.subscribers[umo]
            self._save_subscribers()
            yield event.plain_result("✅ 已取消订阅，本窗口不再接收商人刷新推送。")
        else:
            yield event.plain_result("⚠️ 当前位置尚未订阅。")

    @command("设置商人推送")
    async def set_push_mode(self, event: AstrMessageEvent, mode: str):
        """【分群配置】设置当前群聊的推送模式"""
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            yield event.plain_result("⚠️ 当前群聊尚未订阅商人推送，请先发送 /订阅商人")
            return

        if mode not in ["0", "1", "2"]:
            yield event.plain_result("❌ 参数错误！请输入规范的参数：\n0 -> 全部推送\n1 -> 只匹配到商品推送\n2 -> 完全不推送")
            return

        mode_int = int(mode)
        self.subscribers[umo]["push_mode"] = mode_int
        self._save_subscribers()

        mode_names = {0: "全部推送", 1: "只匹配到商品推送", 2: "完全不推送"}
        yield event.plain_result(f"✅ 设置成功！当前群聊的推送模式已修改为：【{mode_names[mode_int]}】")

    @command("设置商人艾特")
    async def set_mention_everyone(self, event: AstrMessageEvent, status: str):
        """【分群配置】设置推送时是否@全员"""
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            yield event.plain_result("⚠️ 当前群聊尚未订阅商人推送，请先发送 /订阅商人")
            return

        if status not in ["0", "1"]:
            yield event.plain_result("❌ 参数错误！请输入规范的参数：\n0 -> 不@全员\n1 -> @全员")
            return

        status_int = int(status)
        self.subscribers[umo]["mention_everyone"] = status_int
        self._save_subscribers()

        status_names = {0: "不@全员", 1: "@全员"}
        yield event.plain_result(f"✅ 设置成功！当前群聊的艾特状态已修改为：【{status_names[status_int]}】")

    @command("商人")
    async def manual_check(self, event: AstrMessageEvent):
        """机器人快捷指令：手动触发并获取当前商品列表"""
        try:
            # 手动执行不走轮询次数限制，也默认不发全群广播，仅回复当前用户
            result_msg, _ = await self.execute_check(send_alert=False)
            yield event.plain_result(result_msg)
        except Exception as e:
            yield event.plain_result(f"查询失败: {str(e)}")
