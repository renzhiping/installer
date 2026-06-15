"""数据模型 — BaseBinding / Binding / Params / AgentNotifier"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .notifier import BaseNotifier


class BaseBinding:
    """基础绑定 — 仅包含身份信息"""

    def __init__(
        self,
        user_id: str = "",
        platform: str = "",
        channel_id: str = "",
        thread_id: str = "",
    ):
        self.user_id = user_id
        self.platform = platform
        self.channel_id = channel_id
        self.thread_id = thread_id


class Binding(BaseBinding):
    """通知器绑定（参考 V2 NotifierBinding）"""

    def __init__(
        self,
        notifier: "BaseNotifier | None" = None,
        user_id: str = "",
        platform: str = "",
        agent_steering: "BaseNotifier | None" = None,
        channel_id: str = "",
        thread_id: str = "",
    ):
        super().__init__(user_id, platform, channel_id, thread_id)
        self.notifier = notifier
        self.agent_steering = agent_steering

    def resolve_notifier(self, prefer_user: bool | None = None, default_prefer: bool = False):
        """根据优先级返回 (notifier, is_user)，无可用时返回 (None, False)。"""
        if prefer_user is None:
            prefer_user = default_prefer
        first = self.notifier if prefer_user else self.agent_steering
        second = self.agent_steering if prefer_user else self.notifier
        n = first or second
        return n, n is self.notifier

    async def notify(self, user_msg: str, agent_msg: str, prefer_user: bool | None = None, default_prefer: bool = False, file_logger=None) -> bool:
        """向所有可用通知器发送对应的消息（非空判断）。"""
        resolved, _ = self.resolve_notifier(prefer_user, default_prefer)
        if not resolved:
            return False
        # 原单选逻辑：resolved.send_text(self.user_id, user_msg if is_user else agent_msg)

        if self.notifier and user_msg:
            ok = await self.notifier.send_text(self.user_id, user_msg)
            if not ok:
                err = self.notifier.get_last_error() or "unknown"
                if file_logger:
                    file_logger.error(f"通知发送失败(notifier): {err}")
        if self.agent_steering and agent_msg:
            ok = await self.agent_steering.send_text(self.user_id, agent_msg)
            if not ok:
                err = self.agent_steering.get_last_error() or "unknown"
                if file_logger:
                    file_logger.error(f"通知发送失败(agent): {err}")
        return True


# AgentNotifier 已迁移到 notifier.py

class Params:
    """提取后的参数（参考 V2 ExtractedParams）"""

    def __init__(
        self,
        action: str,
        text: str = "",
        task_id: str = "",
        files: list | None = None,
        channel_id: str = "",
        thread_id: str = "",
        context_id: str = "",
    ):
        self.action = action
        self.text = text
        self.task_id = task_id
        self.files = files or []
        self.channel_id = channel_id
        self.thread_id = thread_id
        self.context_id = context_id
