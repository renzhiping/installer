"""合同审核服务 — Hermes 版（参考 V2 PluginService 结构）"""

import asyncio
import json

from .auth import AuthFacade
from .client import A2AClient
from .file_logger import FileLogger
from .models import Binding, Params
from .notifier_service import TaskNotificationManager
from .uploader import UploadService


class PluginService:
    """合同审核服务"""

    def __init__(
        self,
        a2a_url: str,
        auth_url: str,
        rest_url: str,
        notifiers: dict | None = None,
        skip_login=False,
        prefer_user_notify=False,
    ):
        self.auth = AuthFacade(auth_url)
        self.client = A2AClient(a2a_url, lambda: self.auth.ensure_access_token())
        self.uploader = UploadService(rest_url, lambda: self.auth.ensure_access_token())
        self.rest_url = rest_url.rstrip("/")
        self.notifiers = notifiers or {}
        self.skip_login = skip_login
        self.prefer_user_notify = prefer_user_notify
        self.last_task_id: str | None = None
        self.file_logger = FileLogger()
        self.task_notifier = TaskNotificationManager(
            self.client, self.file_logger, prefer_user_notify
        )

    # ── 工具入口（由 __init__.py 的 handler 调用）────────

    async def handle_tool_call(self, binding: Binding, params: Params) -> str:
        """统一入口（参考 V2 handleToolCall）"""
        # 从 notifiers 字典按平台匹配合适的通知器
        if binding.notifier is None:
            binding.notifier = self.notifiers.get(binding.platform)
        elif isinstance(binding.notifier, str):
            binding.notifier = self.notifiers.get(binding.notifier)

        # # # 测试微信发送
        # wx = self.notifiers.get("weixin")
        # if wx:
        #     await wx.send_fn("o9cq808t_PCamA5EjhggGDMm-S4s@im.wechat", "- 第一行\n- 第二行\n- 第三行\n  第四行")
        # return "微信发送测试完成"

        if not self.skip_login and not self.auth.is_logged_in():
            return await self._start_login(binding, params)
        return await self._dispatch_action(binding, params)

    async def _dispatch_action(self, binding: Binding, params: Params) -> str:
        """参考 V2 dispatchAction"""
        if not self.skip_login and not self.auth.is_logged_in():
            return "登录状态已失效，请重新发送请求。"
        
        if params.action == "forward":
            result = await self._handle_forward(binding, params)
        elif params.action == "status":
            result = await self._handle_status(binding, params)
        # elif params.action == "tasks":
        #     result = await self._handle_tasks(binding, params)
        else:
            result = f"暂不支持 {params.action} 操作"
        return result

    @staticmethod
    def _fmt_err(err: Exception) -> str:
        msg = str(err).strip()
        return f"{type(err).__name__}: {msg}" if msg else type(err).__name__

    # ── 登录流程（参考 V2 startLogin + resumeAfterLogin）─

    async def _resume_after_login(self, binding: Binding, params: Params) -> None:
        """登录成功后重放原始请求并推送结果"""
        result = await self._dispatch_action(binding, params)
        await binding.notify(
            f"请求已完成：{result}",
            f"请求已完成,后续处理将通过通知发送：{result}",
            default_prefer=self.prefer_user_notify, file_logger=self.file_logger,
        )

    async def _start_login(self, binding: Binding, params: Params) -> str:
        """启动登录流程（参考 V2 startLogin）"""
        n, is_user = binding.resolve_notifier(default_prefer=self.prefer_user_notify)
        if not n:
            return "没有消息通知通道，无法推送登录链接。"

        async def push_login_url(login_url: str, code: str) -> None:
            await binding.notify(
                f"登录链接：{login_url}\n请在浏览器中打开完成登录。5分钟内有效",
                f"登录链接：{login_url}\n请在浏览器中打开完成登录。5分钟内有效",
                file_logger=self.file_logger,
            )

        async def on_login_result(result: dict) -> None:
            if result.get("ok") or self.auth.is_logged_in():
                if not result.get("ok"):
                    self.file_logger.info(f"登录已由其他请求完成 user={binding.user_id}")
                else:
                    self.file_logger.info(f"登录成功 user={binding.user_id}")
                await self._resume_after_login(binding, params)
            else:
                self.file_logger.warn(
                    f"登录失败 user={binding.user_id} error={result.get('error')}"
                )
                await binding.notify(
                    f"登录失败：{result.get('error', '未知错误')}",
                    f"登录失败：{result.get('error', '未知错误')}",
                    file_logger=self.file_logger,
                )

        # 不 await — 后台运行，on_result 回调完成后自动恢复请求
        asyncio.create_task(self.auth.login(
            context={
                "imPlatform": binding.platform,
                "imUserId": binding.user_id,
                "channelId": binding.channel_id,
                "threadId": binding.thread_id,
            },
            on_notifier=push_login_url,
            on_result=on_login_result,
        ))
        if is_user:
            return "登陆连接已从其他通道推送给用户,等待用户自行完成通知中的登录,若完成登陆会自动完成本次请求,下次调用不再有登陆提示。"
        return "插件消息:请稍等,稍后会登陆连接推送。"

    # ── 业务处理 ────────────────────────────────────────

    async def _handle_forward(self, binding: Binding, params: Params) -> str:
        self.file_logger.info(
            f"forward user={binding.user_id} platform={binding.platform} text={params.text}"
        )

        # 1. 上传附件
        uploaded_files: list[dict] = []
        for f in params.files:
            try:
                uploaded = await self.uploader.upload_attachment(
                    f.get("filename", ""), f.get("path", "")
                )
                uploaded_files.append({
                    "fileId": uploaded["fileId"],
                    "filename": uploaded["filename"],
                    "mediaType": uploaded.get("contentType", "application/octet-stream"),
                })
            except Exception as err:
                reason = self._fmt_err(err)
                self.file_logger.error(f"文件上传失败 {f.get('filename', '')}: {reason}")
                return f'文件 "{f.get("filename", "")}" 上传失败：{reason}'

        # 2. 提交任务（最多重试一次：若 last_task_id 指向终态任务则清空重试）
        import uuid
        for attempt in range(2):
            forward_task_id = params.task_id or (self.last_task_id if attempt == 0 else "")
            if attempt == 0:
                forward_task_id = params.task_id or self.last_task_id or ""
            else:
                forward_task_id = params.task_id or ""
                self.last_task_id = None  # 清空终态任务引用
            context_id = params.context_id or str(uuid.uuid4())
            try:
                result = await self.client.forward(
                    text=params.text,
                    files=uploaded_files if uploaded_files else None,
                    platform=binding.platform,
                    channel_id=binding.channel_id or binding.platform,
                    thread_id=binding.thread_id,
                    context_id=context_id,
                    task_id=forward_task_id,
                )
                break  # 成功，跳出重试循环
            except Exception as err:
                err_msg = self._fmt_err(err)
                is_terminal = "terminal state" in err_msg.lower()
                if is_terminal and attempt == 0 and self.last_task_id and not params.task_id:
                    self.file_logger.warn(f"forward 任务 {self.last_task_id} 已终止，清空引用后重试")
                    continue
                self.file_logger.error(f"forward 请求失败: {err_msg}")
                return f"请求失败：{err_msg}"

        # 3. 启动后台 SSE 通知
        self.last_task_id = result.taskId
        notified = self.task_notifier.start(
            self.last_task_id,
            binding,
            on_terminal=lambda: setattr(self, "last_task_id", None),
        )

        msg = f"已提交，任务 ID：{result.taskId}"
        if not notified:
            msg += "（无消息返回通道，无法接受后续通知）"
        return msg

    async def _handle_status(self, binding: Binding, params: Params) -> str:
        self.file_logger.info(
            f"status user={binding.user_id} platform={binding.platform} task={params.task_id}"
        )

        target_task_id = params.task_id or self.last_task_id
        if not target_task_id:
            return "请先提交任务后再查询状态。"

        try:
            snapshot = await self.client.get_status(target_task_id)
            return (
                f"任务 {snapshot.taskId} 状态：{snapshot.status}"
                f"（阶段：{snapshot.stage or '无'}）"
            )
        except Exception as err:
            self.file_logger.error(f"status 查询失败 task={target_task_id}: {self._fmt_err(err)}")
            return f"查询任务状态失败：{self._fmt_err(err)}"

    async def _handle_tasks(self, binding: Binding, params: Params) -> str:
        self.file_logger.info(f"tasks user={binding.user_id} platform={binding.platform}")

        try:
            result = await self.client.list_tasks()
            return json.dumps({
                "tasks": result.tasks,
                "nextPageToken": result.nextPageToken,
            }, ensure_ascii=False, indent=2)
        except Exception as err:
            self.file_logger.error(f"tasks 查询失败: {self._fmt_err(err)}")
            return f"查询任务列表失败：{self._fmt_err(err)}"
