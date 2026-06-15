"""SSE 任务通知服务 — Hermes 版（参考 V2 services/task-notification-manager.ts）"""

import asyncio
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import A2AClient

from .models import Binding


class TaskNotificationManager:
    """SSE 任务通知管理器

    后台订阅任务事件流，自动格式化并推送飞书通知。
    支持心跳防抖、超时重连、工作项追踪。
    """

    def __init__(self, a2a_client: "A2AClient", file_logger=None, prefer_user_notify=False):
        self._a2a_client = a2a_client
        self._file_logger = file_logger
        self._prefer_user_notify = prefer_user_notify
        self._active_tasks: dict[str, bool] = {}

    # ── 节点名称重映射 ───────────────────────────────────

    @staticmethod
    def _remap_node(name: str) -> str:
        mapping = {
            "parse": "解析文档",
            "contract_type_analyze": "分析合同类型",
            "checklist_template_prepare": "准备清单模板",
            "review_checklist_generate": "生成审核清单",
            "review_execute": "执行审核",
            "plan_review_work": "规划审核工作",
        }
        return mapping.get(name, name)

    @staticmethod
    def _remap_node_status(status: str) -> str:
        mapping = {
            "node.started": "节点开始",
            "node.completed": "节点完成",
            "batch.started": "batch:started",
            "batch.completed": "batch:completed",
            "evaluation.completed": "evaluation:completed",
            "plan.ready": "plan:ready",
            "waiting_user.entered": "waiting:entered",
            "result.published": "已发布结果",
            "task.heartbeat": "heartbeat",
        }
        return mapping.get(status, status)

    # ── 外部入口 ──────────────────────────────────────────

    def start(self, task_id: str, binding: Binding, on_terminal=None) -> bool:
        """注册任务并启动后台 SSE 订阅。返回 False 表示无可用通知器。"""
        if not binding.notifier and not binding.agent_steering:
            return False
        key = f"{binding.user_id}:{task_id}"
        if key in self._active_tasks:
            return True

        self._active_tasks[key] = True
        asyncio.create_task(
            self._subscribe_with_retry(task_id, binding, key, on_terminal)
        )
        return True

    # ── 重试循环 ──────────────────────────────────────────

    async def _subscribe_with_retry(
        self,
        task_id: str,
        binding: Binding,
        context_key: str,
        on_terminal=None,
    ):
        """超时自动重连，最多 3 次，每次间隔 10 秒。结束时调用 on_terminal()。"""
        work_plan_items: list[dict] = []

        try:
            for attempt in range(1, 4):
                try:
                    await self._subscribe_and_notify(task_id, binding, work_plan_items)
                    # 正常结束
                    await binding.notify(f"任务 {task_id} 通知订阅已结束", f"任务 {task_id} 通知订阅已结束", default_prefer=self._prefer_user_notify, file_logger=self._file_logger)
                    break
                except Exception as err:
                    err_str = str(err)
                    is_timeout = "timeout" in err_str.lower() or "timed out" in err_str.lower()

                    if is_timeout and attempt < 3:
                        await asyncio.sleep(10)
                        continue

                    reason = "网络连接超时（SSE 流空闲超时）" if is_timeout else err_str
                    await binding.notify(f"任务 {task_id} 通知订阅已断开：{reason}", f"任务 {task_id} 通知订阅已断开：{reason}", default_prefer=self._prefer_user_notify, file_logger=self._file_logger)
                    break
        except Exception as e:
            if self._file_logger:
                self._file_logger.error(f"_subscribe_with_retry 崩溃 task={task_id}: {type(e).__name__}: {e}")

        self._active_tasks.pop(context_key, None)
        if on_terminal:
            on_terminal()


    # ── 事件格式化 ────────────────────────────────────────

    @staticmethod
    def _get_event_field(event: dict, *keys: str | tuple):
        """从事件字典中取值，支持 camelCase / snake_case 以及嵌套路径。

        每个 key 可以是：
        - str → event[key] 顶层取值
        - (parent, child) → event[parent][child] 嵌套取值
        """
        for key in keys:
            if isinstance(key, tuple):
                parent, child = key
                inner = event.get(parent) if isinstance(event.get(parent), dict) else None
                val = inner.get(child) if inner else None
            else:
                val = event.get(key)
            if val is not None:
                return val
        return None

    def _format_event(self, event: dict, work_plan_items: list[dict] | None = None) -> str | None:
        """格式化事件为通知文本"""
        task_id = self._get_event_field(event, "taskId", "task_id")
        if not task_id:
            return None

        state = self._get_event_field(event, "state")
        stage = self._get_event_field(event, "stage")
        kind = self._get_event_field(event, "kind")
        work_plan = self._get_event_field(event, "work_plan")
        event_type = self._get_event_field(event, "event_type", "eventType")
        display_name = self._get_event_field(event, "display_name", "displayName")
        path = self._get_event_field(event, "path")
        message = self._get_event_field(event, "message")

        # 输入请求事件
        if state == "TASK_STATE_INPUT_REQUIRED":
            parts = [f"任务ID: {task_id}"]
            if display_name and path:
                parts.append(f"文件: {display_name}\n地址: {path}")
            if message:
                parts.append(message)
            return "\n".join(parts)

        # 文件事件
        if event_type == "artifact" and display_name and path:
            return (
                f"任务ID: {task_id}\n"
                f"接收到文件: {display_name}\n"
                f"地址: {path}"
            )

        # 普通进度事件
        if not state or not stage or not kind:
            return None

        # 更新工作项
        if stage == "plan_review_work" and isinstance(work_plan, list):
            for item in work_plan:
                if not any(w["name"] == item for w in (work_plan_items or [])):
                    if work_plan_items is not None:
                        work_plan_items.append({"name": item, "status": "pending"})

        extra = ""
        if work_plan_items:
            lines: list[str] = []
            for item in work_plan_items:
                prefix = "✅" if item["status"] == "completed" else "🔄" if item["status"] == "running" else "⬜"
                lines.append(f"{prefix} {self._remap_node(item['name'])}")
            if lines:
                extra = "\n" + "\n".join(lines)

        return (
            f"任务ID: {task_id}\n"
            f"任务状态: {state}\n"
            f"处理节点: {self._remap_node(stage)}\n"
            f"节点状态: {self._remap_node_status(kind)}"
        ) + extra

    # ── SSE 订阅与通知 ────────────────────────────────────
    


    async def _subscribe_and_notify(
        self,
        task_id: str,
        binding: Binding,
        work_plan_items: list[dict],
    ):
        hb_task: asyncio.Task | None = None

        async def _heartbeat_job():
            nonlocal hb_task
            await binding.notify(f"[heartbeat] 任务{task_id}处于活跃状态", f"[heartbeat] 任务{task_id}处于活跃状态", default_prefer=self._prefer_user_notify, file_logger=self._file_logger)
            await asyncio.sleep(500)
            hb_task = None

        try:
            async for event in self._a2a_client.subscribe_stream(task_id):
                if self._file_logger:
                    self._file_logger.info(f"SSE原始数据: {json.dumps(event, ensure_ascii=False)}")
                task_status = self._get_event_field(event, "state", ("status", "state")) or ""
                event_type = self._get_event_field(event, "event_type", "eventType")
                heartbeat = event_type == "task.heartbeat"

                # 心跳防抖 300 秒
                if heartbeat:
                    if hb_task is None:
                        hb_task = asyncio.create_task(_heartbeat_job())
                    continue

                # 业务消息到达，取消心跳
                if hb_task is not None:
                    hb_task.cancel()
                    hb_task = None

                # 更新工作项状态
                kind = self._get_event_field(event, "kind")
                stage = self._get_event_field(event, "stage")
                if kind == "node.started" and stage:
                    for item in work_plan_items:
                        if item["name"] == stage:
                            item["status"] = "running"
                            break
                if kind == "node.completed" and stage:
                    for item in work_plan_items:
                        if item["name"] == stage:
                            item["status"] = "completed"
                            break

                # 过滤不需要通知的事件
                filtered_kinds = {"batch.started", "evaluation.completed"}
                if kind in filtered_kinds:
                    continue

                # 日志 + 发送通知
                if self._file_logger:
                    self._file_logger.info(json.dumps(event, ensure_ascii=False))
                summary = self._format_event(event, work_plan_items) or json.dumps(event, ensure_ascii=False)
                await binding.notify(summary, summary, default_prefer=self._prefer_user_notify, file_logger=self._file_logger)

                # 终端状态退出
                if task_status in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED"):
                    break

        except Exception as ev:
            if self._file_logger:
                self._file_logger.error(f"SSE事件处理异常: {type(ev).__name__}: {ev}")
            raise

        finally:
            if hb_task is not None:
                hb_task.cancel()
