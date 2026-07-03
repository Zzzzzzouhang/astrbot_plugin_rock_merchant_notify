"""
远行商人监控核心逻辑模块

包含：网页抓取、HTML 解析、轮询调度、历史记录管理、窗口防重复推送。
本模块不依赖 AstrBot，可独立测试和复用。
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Awaitable
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


# ==================== 日志工具 ====================

def setup_file_logger(name: str, log_path: Path) -> logging.Logger:
    """为插件配置独立的文件日志（追加模式，不删除已有日志）"""
    logger = logging.getLogger(f"{name}.file")
    logger.setLevel(logging.INFO)
    logger.disabled = False
    logger.propagate = False

    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")  # mode='a' 默认追加
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(fh)

    return logger


# ==================== 纯函数：抓取与解析 ====================

def fetch_page(url: str) -> str:
    """抓取远行商人页面 HTML"""
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
    """解析当前激活的时间槽位"""
    time_items = soup.select(".time-list li")
    if not time_items:
        raise ValueError("Failed to find time slots in merchant page")

    active_items = [el for el in time_items if "on" in el.get("class", [])]

    if len(active_items) == 1:
        element = active_items[0]
        index = time_items.index(element) + 1
        values = [em.get_text(strip=True) for em in element.select("em")]
        start = values[0] if len(values) > 0 else ""
        end = values[1] if len(values) > 1 else ""
        return {
            "index": int(element.get("data-index", index)),
            "label": f"{start}-{end}" if start and end else f"slot-{index}",
            "start": start,
            "end": end,
        }

    if len(active_items) == 0:
        element = time_items[0]
        values = [em.get_text(strip=True) for em in element.select("em")]
        return {
            "index": int(element.get("data-index", 1)),
            "label": f"{values[0]}-{values[1]}" if len(values) >= 2 else "slot-1",
            "start": values[0] if len(values) >= 1 else "",
            "end": values[1] if len(values) >= 2 else "",
        }

    raise ValueError(f"Multiple active slots detected ({len(active_items)} found), page state ambiguous")


def parse_items(html: str) -> dict[str, Any]:
    """解析页面中的商品列表"""
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
    """提取用于比对的核心字段（排除 fetched_at 等时间戳）"""
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
    """判断数据是否发生变化"""
    if previous is None:
        return True
    return comparable_payload(previous) != comparable_payload(current)


# ==================== 核心监控器 ====================

class MerchantMonitor:
    """远行商人监控器：负责轮询抓取、状态比对、历史记录、窗口防重复。

    通过 push_callback 将推送逻辑委托给调用方（如 AstrBot 插件适配层）。
    """

    def __init__(
        self,
        data_dir: Path,
        config: dict,
        logger: logging.Logger,
        push_callback: Callable[[str, list[str]], Awaitable[None]] | None = None,
    ):
        self.data_dir = data_dir
        self.config = config
        self.logger = logger
        self.push_callback = push_callback
        self.timezone = "Asia/Shanghai"

        self.state_path = self.data_dir / "latest.json"
        self.history_path = self.data_dir / "history.jsonl"

        # 轮询状态
        self._active_slot: str | None = None
        self._attempt_count: int = 0
        self._last_fetch_result: dict[str, Any] | None = None

        # 内存标记：窗口推送去重（格式 "YYYYMMDD_slot"，重载自然清空）
        self._pushed_windows: set[str] = set()
        # 停止标志
        self._stopped: bool = False

        # 文件写入锁
        self._file_lock = asyncio.Lock()

        self.logger.info("MerchantMonitor 已启动")

        # 启动后台轮询
        self._polling_task = asyncio.create_task(self.poll_loop())

    # ---------- 配置读取 ----------

    def get_watch_items(self) -> list[str]:
        raw = self.config.get("watch_items", "国王球,棱镜球,炫彩蛋,炫彩精灵蛋,祝福吊坠")
        return [i.strip() for i in raw.split(",") if i.strip()]

    def get_active_slot(self, current_time: datetime) -> str | None:
        """获取当前时间是否在任意配置的窗口期内，返回槽位时间字符串或 None"""
        current_minutes = current_time.hour * 60 + current_time.minute
        window = self.config.get("refresh_window_minutes", 60)
        times_str = self.config.get("refresh_times", "08:01,12:01,16:01,20:01")

        for item in [t.strip() for t in times_str.split(",") if t.strip()]:
            hour_str, minute_str = item.split(":", 1)
            scheduled_minutes = int(hour_str) * 60 + int(minute_str)

            diff = current_minutes - scheduled_minutes
            if diff < 0 and current_minutes < window:
                diff += 1440
            if 0 <= diff <= window:
                return item
        return None

    # ---------- 窗口标记（内存版，重载自然清空）----------

    def _window_key(self, slot: str) -> str:
        today = datetime.now(ZoneInfo(self.timezone)).strftime("%Y%m%d")
        return f"{today}_{slot}"

    def _is_window_pushed(self, slot: str) -> bool:
        return self._window_key(slot) in self._pushed_windows

    def _mark_window_pushed(self, slot: str) -> None:
        self._pushed_windows.add(self._window_key(slot))

    # ---------- 抓取 ----------

    async def fetch(self) -> dict[str, Any]:
        """纯抓取+解析，返回 payload（不含比对的单次抓取）"""
        current_time = datetime.now(ZoneInfo(self.timezone))
        url = self.config.get("merchant_url", "")
        if not url:
            raise ValueError("配置错误：未提供 merchant_url")

        html = await asyncio.to_thread(fetch_page, url)
        parsed = await asyncio.to_thread(parse_items, html)

        return {
            "fetched_at": current_time.isoformat(),
            "slot": parsed["slot"],
            "items": parsed["items"],
        }

    async def execute_fetch_only(self) -> tuple[dict[str, Any], bool]:
        """兼容包装：抓取并返回 (payload, changed)。供 main.py 手动查询使用。"""
        payload = await self.fetch()
        previous = self._read_state()
        changed = has_changed(previous, payload)
        return payload, changed

    def _read_state(self) -> dict[str, Any] | None:
        """读取 latest.json 中的上次保存状态（统一入口）"""
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                self.logger.error(f"读取状态文件失败: {e}")
        return None

    def get_cached_payload(self) -> dict[str, Any] | None:
        """读取缓存的商品数据（供指令快速响应，委托 _read_state）"""
        return self._read_state()

    # ---------- 消息构建 ----------

    def build_message(self, payload: dict[str, Any], display_time: datetime) -> tuple[str, list[str]]:
        """构建推送消息文本，返回 (完整消息, 匹配商品列表)"""
        watch_items = self.get_watch_items()
        matched = [item["name"] for item in payload["items"] if item["name"] in watch_items]

        slot_label = payload["slot"]["label"]
        title = f"远行商人已刷新 {slot_label}" if not matched else f"远行商人命中关注商品 {slot_label}"

        lines = []
        if matched:
            lines.append(f"🎯 关注商品：{', '.join(matched)}")
        for item in payload["items"]:
            price = item["price_text"] or "未知价格"
            limit = item["limit_text"] or "限购未知"
            lines.append(f"{item['slot']}. {item['name']} | {price} | {limit}")

        message = "\n".join(lines)
        time_str = display_time.strftime("%Y-%m-%d %H:%M")
        full_text = f"【{title}】\n{message}\n\n🕒 检查时间：{time_str}"

        return full_text, matched

    # ---------- 历史记录 ----------

    async def _save_state(self, payload: dict[str, Any]):
        """只写 latest.json"""
        async with self._file_lock:
            self.state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    async def _append_history(self, payload: dict[str, Any]):
        """追加 history.jsonl（O(1) 仅与末行比对去重，低频滚动清理保持上限 1000 条）"""
        async with self._file_lock:
            should_append = True
            last_line = ""

            if self.history_path.exists():
                try:
                    with self.history_path.open("r", encoding="utf-8") as fp:
                        for line in fp:
                            last_line = line
                    if last_line:
                        last_entry = json.loads(last_line.strip())
                        if comparable_payload(last_entry) == comparable_payload(payload):
                            self.logger.debug("历史记录已存在相同内容，跳过重复写入")
                            should_append = False
                except Exception as e:
                    self.logger.error(f"读取历史记录失败: {e}")

            if should_append:
                new_line = json.dumps(payload, ensure_ascii=False) + "\n"
                with self.history_path.open("a", encoding="utf-8") as fp:
                    fp.write(new_line)

                # 低频滚动清理：只在与末行比对时判断是否超限
                try:
                    line_count = sum(1 for _ in self.history_path.open("r", encoding="utf-8"))
                    if line_count > 1000:
                        with self.history_path.open("r", encoding="utf-8") as fp:
                            lines = fp.readlines()
                        with self.history_path.open("w", encoding="utf-8") as fp:
                            fp.writelines(lines[-1000:])
                except Exception as e:
                    self.logger.error(f"历史记录滚动清理失败: {e}")

    def get_history_lines(self) -> list[str]:
        """读取所有历史记录行"""
        if not self.history_path.exists():
            return []
        try:
            with self.history_path.open("r", encoding="utf-8") as fp:
                return fp.readlines()
        except Exception as e:
            self.logger.error(f"读取历史记录失败: {e}")
            return []

    # ---------- 推送 ----------

    async def _push(self, payload: dict[str, Any], send_alert: bool) -> None:
        """构建消息并通过 callback 推送"""
        display_time = datetime.now(ZoneInfo(self.timezone))
        full_text, matched = self.build_message(payload, display_time)
        if send_alert and self.push_callback:
            await self.push_callback(full_text, matched)

    # ---------- 轮询主循环 ----------

    async def poll_loop(self):
        """精简主循环：窗口检测 → 委托 _handle_window → sleep"""
        while not self._stopped:
            try:
                current_time = datetime.now(ZoneInfo(self.timezone))
                active_slot = self.get_active_slot(current_time)

                if active_slot:
                    await self._handle_window(active_slot)
                elif self._active_slot is not None:
                    self.logger.info("窗口期结束，清理轮询状态")
                    self._active_slot = None
                    self._attempt_count = 0
                    self._last_fetch_result = None

            except asyncio.CancelledError:
                self.logger.info("轮询任务被取消，正常退出")
                break
            except Exception as e:
                self.logger.error(f"Polling error: {e}", exc_info=True)

            await asyncio.sleep(120)

    async def _handle_window(self, slot: str) -> None:
        """单窗口连续两次比对完整流程（重置状态 → 5次抓取 → 比对 → 推送 → 熔断兜底）"""
        if slot != self._active_slot:
            self._active_slot = slot
            self._attempt_count = 0
            self._last_fetch_result = None
            self.logger.info(f"进入新窗口期 {slot}，重置轮询状态")

        if self._is_window_pushed(slot):
            self.logger.info(f"{slot} 本窗口已推送过，跳过")
            return
        if self._attempt_count >= 5:
            self.logger.info(f"{slot} 已达最大重试次数，等待新窗口")
            return

        self._attempt_count += 1
        self.logger.info(f"{slot} 窗口期第 {self._attempt_count}/5 次抓取检测...")

        try:
            payload = await self.fetch()
        except Exception as e:
            self.logger.warning(f"{slot} 第 {self._attempt_count} 次抓取失败: {e}")
            if self._attempt_count >= 5:
                self.logger.warning(f"{slot} 窗口期内所有抓取均失败，本窗口放弃")
                # 标记已尽力，避免后续轮询空转
                self._mark_window_pushed(slot)
            return

        last = self._last_fetch_result
        matched = last is not None and comparable_payload(last) == comparable_payload(payload)

        if matched:
            # 连续两次抓取结果一致 → 推送
            self.logger.info(f"{slot} 连续两次抓取结果一致，触发推送")
            await self._save_state(payload)
            await self._append_history(payload)
            await self._push(payload, send_alert=True)
            # 先持久化再标记，避免中间异常导致窗口被标记但数据丢失
            self._mark_window_pushed(slot)
            self.logger.info(f"{slot} 推送完毕，本窗口期不再继续抓取。")
        elif self._attempt_count >= 5:
            # 兜底：已达最大次数，使用最后一次结果推送
            self.logger.info(f"{slot} 已达最大检测次数，使用最后一次结果兜底推送")
            await self._save_state(payload)
            await self._append_history(payload)
            await self._push(payload, send_alert=True)
            self._mark_window_pushed(slot)
            self.logger.info(f"{slot} 兜底推送完毕，本窗口期不再继续抓取。")
        else:
            self._last_fetch_result = payload
            if last is None:
                self.logger.info(f"{slot} 第 {self._attempt_count} 次抓取完成，等待下次抓取比对...")
            else:
                self.logger.info(f"{slot} 第 {self._attempt_count} 次抓取结果与上次不一致，继续比对...")

    # ---------- 生命周期 ----------

    async def stop(self):
        """停止轮询并释放资源"""
        self._stopped = True

        if hasattr(self, "_polling_task") and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                # 超时保护：避免 poll_loop 阻塞在 asyncio.to_thread 中影响框架重载
                await asyncio.wait_for(self._polling_task, timeout=10)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        self.logger.info("Monitor 已停止")
