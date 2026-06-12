"""
远行商人监控插件 — AstrBot 适配层

本文件只负责：
  - AstrBot 指令注册与响应
  - 订阅者管理（分群配置）
  - 推送分发（通过 MessageChain）

核心抓取/解析/轮询/历史逻辑见 merchant.py
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from astrbot.api.all import Star, Context, register, command, AstrMessageEvent
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from astrbot.api.event import MessageChain

from merchant import MerchantMonitor, setup_file_logger


@register(
    "astrbot_plugin_rock_merchant_notify",
    "AstrBot",
    "远行商人监控插件",
    "1.4.0",
    "支持不同群聊独立配置推送模式及艾特规则",
)
class MerchantNotifyPlugin(Star):
    HINT_LINE = "\n💡 /远行商人 /「取消」订阅商人  /商人历史"

    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config

        # 数据目录
        self.data_dir = Path(get_astrbot_data_path()) / "plugin_data" / self.name
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 日志
        self.logger = setup_file_logger(self.name, self.data_dir / "merchant.log")

        # 订阅者
        self.subscribers_path = self.data_dir / "subscribers.json"
        self.subscribers = self._load_subscribers()
        self._file_lock = asyncio.Lock()

        # 核心监控器（推送通过 callback 委托给本类）
        self.monitor = MerchantMonitor(
            data_dir=self.data_dir,
            config=self.config,
            logger=self.logger,
            push_callback=self._dispatch_push,
        )

    # ==================== 订阅者管理 ====================

    def _load_subscribers(self) -> dict[str, dict]:
        if self.subscribers_path.exists():
            try:
                data = json.loads(self.subscribers_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    return {umo: {"push_mode": 1, "mention_everyone": 0} for umo in data}
                return data
            except Exception as e:
                self.logger.error(f"读取订阅列表失败: {e}")
        return {}

    async def _save_subscribers(self):
        async with self._file_lock:
            self.subscribers_path.write_text(
                json.dumps(self.subscribers, ensure_ascii=False), encoding="utf-8"
            )

    # ==================== 推送分发 ====================

    async def _dispatch_push(self, full_text: str, matched: list[str]):
        """push_callback：向所有符合条件的订阅者分发消息"""
        if not self.subscribers:
            return

        push_interval = self.config.get("push_interval_seconds", 5)
        self.logger.info(f"开始处理分群过滤推送，间隔 {push_interval} 秒...")

        for umo, settings in self.subscribers.items():
            push_mode = settings.get("push_mode", 1)
            mention_everyone = settings.get("mention_everyone", 0)

            should_push = False
            if push_mode == 0:
                should_push = True
            elif push_mode == 1 and matched:
                should_push = True
            # push_mode == 2 → 完全不推送

            if should_push:
                chain = MessageChain()
                if mention_everyone == 1:
                    chain.at_all()
                chain.message(full_text)
                await self._push_with_retry(umo, chain)
                await asyncio.sleep(push_interval)

    async def _push_with_retry(self, umo: str, chain: MessageChain) -> bool:
        max_retries = 3
        retry_delay = 5
        for attempt in range(1, max_retries + 1):
            try:
                await self.context.send_message(umo, chain)
                self.logger.info(f"已向 {umo} 发送推送")
                return True
            except Exception as e:
                self.logger.error(
                    f"向 {umo} 推送失败 (第 {attempt}/{max_retries} 次): {e}",
                    exc_info=True,
                )
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(f"向 {umo} 推送已达最大重试次数，放弃推送。")
        return False

    # ==================== 权限校验 ====================

    def _check_admin(self, event: AstrMessageEvent) -> bool:
        """群聊仅管理员/群主可用，私聊直接放行"""
        if not event.get_group_id():
            return True
        return event.is_admin()

    # ==================== AstrBot 指令 ====================

    @command("订阅商人")
    async def subscribe(self, event: AstrMessageEvent):
        """订阅远行商人刷新自动推送"""
        if not self._check_admin(event):
            yield event.plain_result("🔒 此指令仅限群管理员/群主使用，私聊无限制。" + self.HINT_LINE)
            return
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            self.subscribers[umo] = {"push_mode": 1, "mention_everyone": 0}
            await self._save_subscribers()
            yield event.plain_result(
                "✅ 订阅成功！远行商人刷新时本窗口将自动收到推送。\n\n"
                "💡 当前群聊初始默认规则：\n"
                "- 推送模式：【1】只匹配到商品推送\n"
                "- 艾特模式：【0】不@全员\n\n"
                "⚙️ 群内可用管理指令：\n"
                "- /设置商人推送 [0/1/2]\n"
                "- /设置商人艾特 [0/1]"
                + self.HINT_LINE
            )
        else:
            s = self.subscribers[umo]
            push_mode = s.get("push_mode", 1)
            mention_everyone = s.get("mention_everyone", 0)
            push_mode_names = {0: "全部推送", 1: "只匹配到商品推送", 2: "完全不推送"}
            mention_names = {0: "不@全员", 1: "@全员"}
            yield event.plain_result(
                "⚠️ 当前位置已经订阅过了，无需重复订阅。\n\n"
                f"💡 当前群聊推送规则：\n"
                f"- 推送模式：【{push_mode}】{push_mode_names.get(push_mode, '未知')}\n"
                f"- 艾特模式：【{mention_everyone}】{mention_names.get(mention_everyone, '未知')}\n\n"
                "⚙️ 群内可用管理指令：\n"
                "- /设置商人推送 [0/1/2]\n"
                "  0 -> 全部推送\n"
                "  1 -> 只匹配到商品推送\n"
                "  2 -> 完全不推送\n"
                "- /设置商人艾特 [0/1]\n"
                "  0 -> 不@全员\n"
                "  1 -> @全员"
                + self.HINT_LINE
            )

    @command("取消订阅商人")
    async def unsubscribe(self, event: AstrMessageEvent):
        """取消订阅远行商人刷新推送"""
        if not self._check_admin(event):
            yield event.plain_result("🔒 此指令仅限群管理员/群主使用，私聊无限制。" + self.HINT_LINE)
            return
        umo = event.unified_msg_origin
        if umo in self.subscribers:
            del self.subscribers[umo]
            await self._save_subscribers()
            yield event.plain_result("✅ 已取消订阅，本窗口不再接收商人刷新推送。" + self.HINT_LINE)
        else:
            yield event.plain_result("⚠️ 当前位置尚未订阅。" + self.HINT_LINE)

    @command("设置商人推送")
    async def set_push_mode(self, event: AstrMessageEvent, mode: str):
        """【分群配置】设置当前群聊的推送模式"""
        if not self._check_admin(event):
            yield event.plain_result("🔒 此指令仅限群管理员/群主使用，私聊无限制。" + self.HINT_LINE)
            return
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            yield event.plain_result("⚠️ 当前群聊尚未订阅商人推送，请先发送 /订阅商人" + self.HINT_LINE)
            return
        if mode not in ["0", "1", "2"]:
            yield event.plain_result("❌ 参数错误！请输入规范的参数：\n0 -> 全部推送\n1 -> 只匹配到商品推送\n2 -> 完全不推送" + self.HINT_LINE)
            return

        mode_int = int(mode)
        self.subscribers[umo]["push_mode"] = mode_int
        await self._save_subscribers()
        mode_names = {0: "全部推送", 1: "只匹配到商品推送", 2: "完全不推送"}
        yield event.plain_result(f"✅ 设置成功！当前群聊的推送模式已修改为：【{mode_names[mode_int]}】" + self.HINT_LINE)

    @command("设置商人艾特")
    async def set_mention_everyone(self, event: AstrMessageEvent, status: str):
        """【分群配置】设置推送时是否@全员"""
        if not self._check_admin(event):
            yield event.plain_result("🔒 此指令仅限群管理员/群主使用，私聊无限制。" + self.HINT_LINE)
            return
        umo = event.unified_msg_origin
        if umo not in self.subscribers:
            yield event.plain_result("⚠️ 当前群聊尚未订阅商人推送，请先发送 /订阅商人" + self.HINT_LINE)
            return
        if status not in ["0", "1"]:
            yield event.plain_result("❌ 参数错误！请输入规范的参数：\n0 -> 不@全员\n1 -> @全员" + self.HINT_LINE)
            return

        status_int = int(status)
        self.subscribers[umo]["mention_everyone"] = status_int
        await self._save_subscribers()
        status_names = {0: "不@全员", 1: "@全员"}
        yield event.plain_result(f"✅ 设置成功！当前群聊的艾特状态已修改为：【{status_names[status_int]}】" + self.HINT_LINE)

    @command("远行商人")
    async def manual_check(self, event: AstrMessageEvent):
        """机器人快捷指令：直接返回当前缓存的商品数据，无缓存时实时抓取"""
        try:
            payload = self.monitor.get_cached_payload()

            if payload is None:
                # 无缓存，实时抓取
                payload, _ = await self.monitor.execute_fetch_only()

            try:
                display_time = datetime.fromisoformat(payload.get("fetched_at", ""))
            except Exception:
                display_time = datetime.now(ZoneInfo(self.monitor.timezone))

            result_msg, _ = self.monitor.build_message(payload, display_time)
            yield event.plain_result(result_msg + self.HINT_LINE)
        except Exception as e:
            self.logger.error(f"手动查询异常: {e}", exc_info=True)
            yield event.plain_result(f"查询失败: {str(e)}" + self.HINT_LINE)

    @command("商人历史")
    async def merchant_history(self, event: AstrMessageEvent):
        """查看远行商人历史刷新记录（最近 15 条）"""
        history_lines = self.monitor.get_history_lines()
        if not history_lines:
            yield event.plain_result("⚠️ 暂无历史记录。" + self.HINT_LINE)
            return

        recent_lines = history_lines[-15:]
        recent_lines.reverse()

        lines = []
        for line in recent_lines:
            try:
                entry = json.loads(line)
                fetched_at = entry.get("fetched_at", "")
                items = entry.get("items", [])
                item_names = [item.get("name", "") for item in items if item.get("name")]

                try:
                    display_time = datetime.fromisoformat(fetched_at)
                    time_str = display_time.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    time_str = fetched_at or "未知时间"

                lines.append(f"[{time_str}] [{', '.join(item_names) if item_names else '空'}]")
            except Exception as e:
                self.logger.error(f"解析历史记录失败: {e}")
                continue

        if not lines:
            yield event.plain_result("⚠️ 历史记录解析失败。" + self.HINT_LINE)
            return

        result = "📜 远行商人历史记录（最近 15 条）：\n\n" + "\n".join(lines)
        yield event.plain_result(result + self.HINT_LINE)

    # ==================== 生命周期 ====================

    async def terminate(self):
        """插件卸载/停用/重载时由 AstrBot 调用"""
        await self.monitor.stop()
