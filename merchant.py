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
    """为插件配置独立的文件日志。

    使用专属 logger 名称（{name}.file）避免与 AstrBot 框架的 logger 冲突。
    每次调用都会清理旧的 handler 并重新创建，确保日志写入正确的文件路径。
    """
    logger_name = f"{name}.file"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.disabled = False
    logger.propagate = False

    # 移除所有旧的 FileHandler，避免指向已删除的旧文件
    for h in list(logger.handlers):
        if isinstance(h, logging.FileHandler):
            h.close()
            logger.removeHandler(h)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(formatter)
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

        # 文件写入锁
        self._file_lock = asyncio.Lock()

        # 清理上一个实例可能遗留的 owner 文件（插件重载场景）
        self._cleanup_orphan_owner()

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

    # ---------- 实例生命周期 ----------

    def _cleanup_orphan_owner(self) -> None:
        """启动时清理上一个实例可能遗留的 stop_flag"""
        flag_path = self.data_dir / ".stop_flag"
        if flag_path.exists():
            try:
                flag_path.unlink()
                self.logger.info("已清理上一个实例的 stop_flag")
            except Exception:
                pass

    # ---------- 窗口标记 ----------

    def _window_flag_path(self, slot_key: str) -> Path:
        today = datetime.now(ZoneInfo(self.timezone)).strftime("%Y%m%d")
        safe_slot = slot_key.replace(":", "")
        return self.data_dir / f".poll_pushed_{today}_{safe_slot}.flag"

    # ---------- 抓取与比对 ----------

    async def execute_fetch_only(self) -> tuple[dict[str, Any], bool]:
        """抓取并解析页面，返回 (payload, changed)"""
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

        previous = self._read_state()
        changed = has_changed(previous, payload)
        return payload, changed

    def _read_state(self) -> dict[str, Any] | None:
        """读取 latest.json 中的上次保存状态"""
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                self.logger.error(f"读取历史状态失败: {e}")
        return None

    def get_cached_payload(self) -> dict[str, Any] | None:
        """读取缓存的商品数据（供指令快速响应）"""
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as e:
                self.logger.warning(f"读取缓存失败: {e}")
        return None

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

    async def save_history(self, payload: dict[str, Any]):
        """保存状态到 latest.json 并追加到 history.jsonl（带去重）"""
        async with self._file_lock:
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            history_lines = []
            if self.history_path.exists():
                try:
                    with self.history_path.open("r", encoding="utf-8") as fp:
                        history_lines = fp.readlines()
                except Exception as e:
                    self.logger.error(f"读取历史记录失败: {e}")

            # 去重：使用 comparable_payload 归一化后，在最近 20 条中检查
            should_append = True
            current_comparable = comparable_payload(payload)
            check_count = min(len(history_lines), 20)
            for i in range(len(history_lines) - check_count, len(history_lines)):
                try:
                    entry = json.loads(history_lines[i])
                    if comparable_payload(entry) == current_comparable:
                        should_append = False
                        self.logger.debug("历史记录已存在相同内容，跳过重复写入")
                        break
                except Exception:
                    continue

            if should_append:
                history_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")
                history_lines = history_lines[-1000:]

                try:
                    with self.history_path.open("w", encoding="utf-8") as fp:
                        fp.writelines(history_lines)
                except Exception as e:
                    self.logger.error(f"写入历史记录失败: {e}")

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

    # ---------- 处理与推送 ----------

    async def process_and_push(self, payload: dict[str, Any], send_alert: bool, force_save: bool = False) -> str:
        """处理抓取结果，保存状态并通过 callback 推送"""
        current_time = datetime.now(ZoneInfo(self.timezone))
        previous = self._read_state()
        changed = has_changed(previous, payload)

        if not changed and previous and "fetched_at" in previous:
            try:
                display_time = datetime.fromisoformat(previous["fetched_at"])
            except Exception:
                display_time = current_time
        else:
            display_time = current_time

        full_text, matched = self.build_message(payload, display_time)

        if changed or force_save:
            await self.save_history(payload)
            if send_alert and self.push_callback:
                await self.push_callback(full_text, matched)

        return full_text

    # ---------- 轮询主循环 ----------

    async def poll_loop(self):
        """后台轮询：在窗口期内定时抓取，数据稳定后推送"""
        stop_flag_path = self.data_dir / ".stop_flag"
        
        while True:
            # 检查是否已被要求停止
            if stop_flag_path.exists():
                self.logger.info("检测到 stop_flag，轮询退出")
                break

            try:
                current_time = datetime.now(ZoneInfo(self.timezone))
                active_slot = self.get_active_slot(current_time)

                if not active_slot:
                    if self._active_slot is not None:
                        self.logger.info("窗口期结束，清理轮询状态")
                    self._active_slot = None
                    self._attempt_count = 0
                    self._last_fetch_result = None
                else:
                    if active_slot != self._active_slot:
                        self._active_slot = active_slot
                        self._attempt_count = 0
                        self._last_fetch_result = None
                        self.logger.info(f"进入新窗口期 {active_slot}，重置轮询状态")

                    flag_path = self._window_flag_path(active_slot)
                    if flag_path.exists():
                        self.logger.info(f"{active_slot} 本窗口已推送过，跳过")
                    elif self._attempt_count < 5:
                        self._attempt_count += 1
                        self.logger.info(f"{active_slot} 窗口期第 {self._attempt_count}/5 次抓取检测...")

                        try:
                            current_payload, _ = await self.execute_fetch_only()
                        except Exception as fetch_err:
                            self.logger.warning(f"{active_slot} 第 {self._attempt_count} 次抓取失败: {fetch_err}，等待下次重试")
                            current_payload = None

                        if current_payload is None:
                            if self._attempt_count >= 5:
                                self.logger.warning(
                                    f"{active_slot} 窗口期内所有抓取均失败，本窗口放弃，等待下一个窗口期"
                                )
                        elif self._attempt_count == 1:
                            self._last_fetch_result = current_payload
                            self.logger.info(f"{active_slot} 第1次抓取完成，等待下次抓取比对...")
                        else:
                            last_result = self._last_fetch_result
                            if last_result is not None and comparable_payload(last_result) == comparable_payload(current_payload):
                                # 创建 flag 防止重复推送
                                self._create_push_flag(flag_path, active_slot)
                                self.logger.info(f"{active_slot} 连续两次抓取结果一致，触发推送")
                                await self.process_and_push(current_payload, send_alert=True, force_save=True)
                                self.logger.info(f"{active_slot} 推送完毕，本窗口期不再继续抓取。")
                            else:
                                self._last_fetch_result = current_payload
                                if last_result is None:
                                    self.logger.info(f"{active_slot} 第 {self._attempt_count} 次抓取，上次结果丢失，重新建立基准...")
                                else:
                                    self.logger.info(f"{active_slot} 第 {self._attempt_count} 次抓取结果与上次不一致，继续比对...")

                            if self._attempt_count >= 5 and not flag_path.exists():
                                self._create_push_flag(flag_path, active_slot)
                                self.logger.info(f"{active_slot} 已达到最大检测次数，使用最后一次结果兜底推送")
                                await self.process_and_push(current_payload, send_alert=True, force_save=True)
                                self.logger.info(f"{active_slot} 兜底推送完毕，本窗口期不再继续抓取。")
                    else:
                        self.logger.info(
                            f"{active_slot} 已达最大重试次数，本窗口不再重试，等待新窗口"
                        )

            except asyncio.CancelledError:
                self.logger.info("轮询任务被取消，正常退出")
                break
            except Exception as e:
                self.logger.error(f"Polling error: {e}", exc_info=True)

            await asyncio.sleep(120)

    def _create_push_flag(self, flag_path: Path, slot_key: str) -> None:
        """创建窗口推送标记文件"""
        try:
            flag_path.write_text(json.dumps({
                "slot": slot_key,
                "pushed_at": datetime.now(ZoneInfo(self.timezone)).isoformat()
            }, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            self.logger.warning(f"创建推送标记文件失败: {e}")

    # ---------- 生命周期 ----------

    async def stop(self):
        """停止轮询并释放资源"""
        # 写入 stop_flag 通知 poll_loop 退出
        stop_flag_path = self.data_dir / ".stop_flag"
        try:
            stop_flag_path.write_text("1", encoding="utf-8")
        except Exception:
            pass

        if hasattr(self, "_polling_task") and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

        # 清理 stop_flag
        try:
            stop_flag_path.unlink(missing_ok=True)
        except Exception:
            pass

        self.logger.info("轮询任务已取消，Monitor 已停止")
