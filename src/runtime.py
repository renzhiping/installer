"""运行时 — 持有 PluginService，通过异步事件循环执行"""

import asyncio
import os
import threading

from .notifier import AgentNotifier, FeishuNotifier, WechatNotifier
from .models import Binding, Params
from .service import PluginService


IP = "http://10.10.1.88"

DEFAULTS = {
    "A2A_BASE_URL": f"{IP}:8080/a2a",
    "AUTH_BASE_URL": f"{IP}:3001",
    "AGENT_REST_BASE_URL": f"{IP}:8100",
}


SCHEMA = {
    "name": "contract_review",
    "description": "合同审核 — 提交任务（forward）、查询状态（status）",
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["forward", "status"],
                "description": "操作类型：forward 提交任务，status 查询状态",
            },
            "user_id": {"type": "string", "description": "用户 ID，示例: ou_123456789"},
            "platform": {"type": "string", "description": "平台使用全小写，示例: feishu, weixin(wechat也使用weixin)"},
            "text": {"type": "string", "description": "审核意图文本（forward 时需要）"},
            "task_id": {"type": "string", "description": "任务 ID（status 时需要；forward 时可选，用于续聊）"},
            "files": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "filename": {"type": "string"},
                        "path": {"type": "string"},
                    },
                    "required": ["filename", "path"],
                },
                "description": "文件列表（forward 时需要）",
            },
        },
        "required": ["action", "user_id", "platform"],
    },
}


class PluginRuntime:
    """运行时包装，内部读取环境变量"""

    def __init__(self, ctx):
        self.agent_notifiers: dict[str, AgentNotifier] = {}

        # 构造通知器并入 notifiers 字典
        feishu_id = os.environ.get("FEISHU_APP_ID")
        feishu_secret = os.environ.get("FEISHU_APP_SECRET")
        weixin_token = os.environ.get("WEIXIN_TOKEN")
        weixin_account_id = os.environ.get("WEIXIN_ACCOUNT_ID")
        weixin_base_url = os.environ.get("WEIXIN_BASE_URL")
        weixin_cdn_base_url = os.environ.get("WEIXIN_CDN_BASE_URL")
        notifiers: dict = {}
        if feishu_id and feishu_secret:
            notifiers["feishu"] = FeishuNotifier(feishu_id, feishu_secret)
        # # 版本1：基于 dispatch_tool，不再使用,但代码保留下来以备参考,不要删除
        # if dispatch_tool:
        #     async def _send_wechat(target: str, message: str) -> str:
        #         return dispatch_tool("send_message", {
        #             "action": "send",
        #             "target": "weixin:" + target,
        #             "message": message,
        #         })

        if weixin_token and weixin_account_id:
            async def _send_wechat_direct(target: str, message: str) -> str:
                from gateway.platforms.weixin import send_weixin_direct
                return await send_weixin_direct(
                    extra={
                        "account_id": weixin_account_id or "",
                        "base_url": weixin_base_url or "",
                        "cdn_base_url": weixin_cdn_base_url or "",
                    },
                    token=weixin_token or None,
                    chat_id=target,
                    message=message,
                )

            notifiers["weixin"] = WechatNotifier(_send_wechat_direct)

        self.service = PluginService(
            a2a_url=os.environ.get("CONTRACT_REVIEW_A2A_BASE_URL", DEFAULTS["A2A_BASE_URL"]),
            auth_url=os.environ.get("CONTRACT_REVIEW_AUTH_BASE_URL", DEFAULTS["AUTH_BASE_URL"]),
            rest_url=os.environ.get("CONTRACT_REVIEW_AGENT_REST_BASE_URL", DEFAULTS["AGENT_REST_BASE_URL"]),
            notifiers=notifiers,
        )
        self._async_loop: asyncio.AbstractEventLoop | None = None
        self._async_thread: threading.Thread | None = None
        self._start_async_loop()
        self._register(ctx)
        self.file_logger.info("Hermes PluginRuntime 构造完成")


    def _start_async_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        self._async_loop = loop
        self._async_thread = t

    @property
    def file_logger(self):
        return self.service.file_logger

    # ── agent notifier 管理 ──────────────────────────────

    def _get_agent_notifier(self, platform: str, user_id: str) -> AgentNotifier:
        key = f"{platform}:{user_id}"
        n = self.agent_notifiers.get(key)
        if n is None:
            n = AgentNotifier()
            self.agent_notifiers[key] = n
        return n

    # ── 工具入口（由 handler 调用）────────────────────────

    def handle_tool_call(self, args: dict, **kwargs) -> str:
        """构建 Binding/Params 并在事件循环中执行"""
        _ = kwargs
        platform = args.get("platform", "")
        user_id = args.get("user_id", "")
        binding = Binding(
            agent_steering=self._get_agent_notifier(platform, user_id),
            user_id=user_id,
            platform=platform,
            channel_id=args.get("channel_id", platform),
            thread_id=args.get("thread_id", ""),
        )
        params = Params(
            action=args.get("action", ""),
            text=args.get("text", ""),
            task_id=args.get("task_id") or "",
            files=args.get("files"),
            channel_id=args.get("channel_id", platform),
            thread_id=args.get("thread_id", ""),
            context_id=args.get("context_id", ""),
        )
        future = asyncio.run_coroutine_threadsafe(
            self.service.handle_tool_call(binding, params),
            self._async_loop,
        )
        return future.result()

    # ── hook：注入插件通知上下文 ─────────────────────────

    def inject_plugin_message_context(self, **kwargs) -> str | None:
        """pre_llm_call hook：推送历史通知给 agent，或注入合同关键词提示"""
        key = f"{kwargs.get('platform', '')}:{kwargs.get('sender_id', '')}"
        n = self.agent_notifiers.get(key)
        if n:
            result = n.read_and_clear()
            if result:
                return (
                    "[插件自动注入上下文] " + result
                    + "\n以上是插件历史后台推送消息,用户已经收到过,优先根据最后消息完成用户请求,若用户有回复你应该发送给插件"
                )
        return self._inject_prompt_context(**kwargs)

    def _register(self, ctx):
        ctx.register_tool(
            "contract_review",
            toolset="tools",
            schema=SCHEMA,
            handler=self.handle_tool_call,
            description="合同审核 — 提交任务、查询状态",
        )
        ctx.register_hook("pre_llm_call", lambda **kw: self.inject_plugin_message_context(**kw))

    @staticmethod
    def _inject_prompt_context(**kwargs) -> str | None:
        """检测用户消息是否涉及合同审核，注入 contract_review tool 使用提示。"""
        user_msg = kwargs.get("user_message", "")
        if not isinstance(user_msg, str) or not user_msg.strip():
            return None

        keywords = [
            "合同", "审核", "合约", "契约", "签署", "签合同",
            "contract", "review", "audit", "agreement",
        ]
        if not any(kw in user_msg.lower() for kw in keywords):
            return None

        return (
            "[auto-context] 可使用 contract_review 工具处理合同。完整指南见技能：contract-review"
        )
