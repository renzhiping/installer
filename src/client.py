"""A2A 协议客户端 — Hermes 版（参考 V2 transport/a2a-client.ts + wire-compat-transport.ts）"""

import json
from dataclasses import dataclass
from uuid import uuid4

import httpx


class A2AError(Exception):
    """A2A 请求异常"""

    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class ForwardResult:
    """参考 V2 GatewayAcceptedTask"""
    taskId: str
    status: str
    contextId: str


@dataclass
class StatusResult:
    """任务状态快照"""
    taskId: str
    status: str
    stage: str


@dataclass
class ListTasksResult:
    """任务列表（参考 V2 TaskListResult）"""
    tasks: list
    nextPageToken: str | None


# ── SSE 事件规整化（参考 V2 normalizeStreamEvent）────────

def normalize_stream_event(raw_event: dict) -> dict:
    """将原始 SSE 事件规整化为扁平结构（参考 V2 normalizeStreamEvent）

    原始事件结构：
      { statusUpdate: { taskId, status: { state, message: { parts: [...] } } } }
    或
      { artifactUpdate: { taskId, artifact: { parts: [{ data: { display_name, download_url } }] } } }

    返回扁平 dict（与 V2 NormalizedInboundTaskEvent 对应）：
      { taskId, state, stage, kind, event_type, message, work_plan, display_name, path }
    """
    # ── artifactUpdate ────────────────────────────────
    artifact_update = raw_event.get("artifactUpdate")
    if isinstance(artifact_update, dict):
        artifact = artifact_update.get("artifact", {})
        parts = artifact.get("parts", []) if isinstance(artifact.get("parts"), list) else []
        data = None
        for p in parts:
            if isinstance(p, dict) and isinstance(p.get("data"), dict):
                data = p["data"]
                break
        return {
            "taskId": artifact_update.get("taskId") or artifact_update.get("task_id", ""),
            "state": "TASK_STATE_WORKING",
            "event_type": "artifact",
            "kind": "artifact",
            "stage": None,
            "message": None,
            "work_plan": None,
            "display_name": (
                (data or {}).get("display_name") or (data or {}).get("displayName")
            ),
            "path": (data or {}).get("download_url"),
        }

    # ── statusUpdate ──────────────────────────────────
    su = raw_event.get("statusUpdate")
    if not isinstance(su, dict):
        # 无法识别的格式，原样返回
        return dict(raw_event)

    su_status = su.get("status") if isinstance(su.get("status"), dict) else {}

    task_id = su.get("taskId") or su.get("task_id", "")
    state = su_status.get("state", "")

    # status.message.parts → text + data
    status_msg = su_status.get("message") if isinstance(su_status.get("message"), dict) else {}
    parts = status_msg.get("parts", []) if isinstance(status_msg.get("parts"), list) else []

    message_text = None
    data = None
    for part in parts:
        if not isinstance(part, dict):
            continue
        if "text" in part and isinstance(part["text"], str) and part["text"].strip():
            if message_text is None:
                message_text = part["text"]
        if "data" in part and isinstance(part["data"], dict):
            data = part["data"]

    work_plan_raw = (data or {}).get("work_plan")
    work_plan = work_plan_raw if isinstance(work_plan_raw, list) else None

    preview_artifacts = (data or {}).get("preview_artifacts")
    first_artifact = preview_artifacts[0] if isinstance(preview_artifacts, list) and preview_artifacts else None
    preview_download_url = (first_artifact or {}).get("download_url") if first_artifact else None
    preview_display_name = (first_artifact or {}).get("display_name") if first_artifact else None

    file_url = None
    file_filename = None
    for part in parts:
        if isinstance(part, dict) and "url" in part and "filename" in part:
            file_url = part["url"]
            file_filename = part["filename"]
            break

    return {
        "taskId": task_id,
        "state": state,
        "event_type": (data or {}).get("event_type"),
        "kind": (data or {}).get("kind"),
        "stage": (data or {}).get("stage"),
        "message": message_text,
        "work_plan": work_plan,
        "display_name": preview_display_name or file_filename,
        "path": preview_download_url or file_url
    }


class A2AClient:
    """合同审核后端 A2A 客户端（参考 V2 A2AClient + WireCompatTransport）"""

    def __init__(self, a2a_url: str, ensure_access_token):
        self.a2a_url = a2a_url.rstrip("/")
        self._ensure_access_token = ensure_access_token
        self._http = httpx.AsyncClient(timeout=60)

    # ── 内部请求 ─────────────────────────────────────────

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """发送带认证的 JSON HTTP 请求"""
        token = (await self._ensure_access_token()) or ""
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers["A2A-Version"] = "1.0"
        content_type = kwargs.pop("content_type", "application/json")
        headers.setdefault("Content-Type", content_type)
        url = f"{self.a2a_url}{path}"

        resp = await self._http.request(method, url, headers=headers, **kwargs)

        if resp.status_code >= 400:
            body = await resp.aread()
            raise A2AError(body.decode("utf-8", errors="replace"), resp.status_code)

        return resp.json()

    @staticmethod
    def _unwrap_task(raw: dict) -> dict:
        """从响应中提取 task 对象（兼容 { task: {...} } 包装）"""
        if isinstance(raw, dict) and "task" in raw:
            return raw["task"]
        return raw

    # ── 提交任务（POST /v1/message:send）──────────────────

    async def forward(
        self,
        text: str,
        files: list | None = None,
        platform: str = "",
        channel_id: str = "",
        thread_id: str = "",
        context_id: str = "",
        task_id: str = "",
    ) -> ForwardResult:
        """提交任务（参考 V2 dispatchGatewayMessage → POST /v1/message:send）

        支持传入 task_id 作 followup 续聊（参考 V2 recentTask）：
        - message.taskId = task_id
        - metadata.recentTask = { taskId, contextId }

        Wire 格式（与 V2 wire-message-codec.ts encodeMessageSendParams 一致）：
          role → ROLE_USER（非 user）
          parts 不含 kind 字段（text part 只含 text，file part 只含 url/filename/mediaType）
        """
        parts: list[dict] = []
        if text:
            parts.append({"text": text})
        if files:
            for f in files:
                parts.append({
                    "url": f.get("url", f"internal://files/{f['fileId']}"),
                    "filename": f["filename"],
                    "mediaType": f.get("mediaType", "application/octet-stream"),
                })

        env: dict[str, str] = {
            "platform": platform,
            "channelId": channel_id,
            "threadId": thread_id,
            "contextId": context_id,

        }

        message: dict = {
            "role": "ROLE_USER",
            "messageId": f"msg-{uuid4()}",
            "parts": parts,
            "metadata": {"env": env},
        }
        if context_id:
            message["contextId"] = context_id
        if task_id:
            message["taskId"] = task_id
            recent = {"taskId": task_id}
            if context_id:
                recent["contextId"] = context_id
            message["metadata"]["recentTask"] = recent

        body = {
            "message": message,
            "configuration": {"returnImmediately": True},
        }

        raw = await self._request("POST", "/v1/message:send", json=body)
        task = self._unwrap_task(raw)
        return ForwardResult(
            taskId=task.get("id", ""),
            status=task.get("status", {}).get("state", "submitted"),
            contextId=context_id,
        )

    # ── 查询任务状态（GET /v1/tasks/{id}）─────────────────

    async def get_status(self, task_id: str) -> StatusResult:
        """查询任务状态（参考 V2 getStatus → GET /v1/tasks/{id}）"""
        raw = await self._request("GET", f"/v1/tasks/{task_id}")
        task = self._unwrap_task(raw)
        return StatusResult(
            taskId=task.get("id", ""),
            status=task.get("status", {}).get("state", ""),
            stage=(
                task.get("metadata", {}).get("stage", "")
                if isinstance(task.get("metadata"), dict)
                else ""
            ),
        )

    # ── 查询任务列表（GET /v1/tasks）──────────────────────

    async def list_tasks(
        self,
        status: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> ListTasksResult:
        """查询任务列表（参考 V2 listTasks → GET /v1/tasks）"""
        params: dict[str, str | int] = {}
        if status:
            params["status"] = status
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token:
            params["pageToken"] = page_token

        raw = await self._request("GET", "/v1/tasks", params=params)
        return ListTasksResult(
            tasks=raw.get("tasks", []),
            nextPageToken=raw.get("nextPageToken"),
        )

    # ── SSE 订阅（POST /v1/tasks/{id}:subscribe）──────────

    async def subscribe_stream(self, task_id: str):
        """SSE 订阅任务事件流

        参考 V2 subscribeStream：
        - 原始事件经 normalize_stream_event 规整化为扁平结构
        - 非 A2AError 异常统一包装为 A2AError
        - completed / failed / canceled 事件后自动退出
        """
        token = (await self._ensure_access_token()) or ""
        url = f"{self.a2a_url}/v1/tasks/{task_id}:subscribe"

        try:
            async with self._http.stream(
                "POST",
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "A2A-Version": "1.0",
                    "Content-Type": "application/json",
                },
            ) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    raise A2AError(
                        body.decode("utf-8", errors="replace"), resp.status_code
                    )

                async for line in resp.aiter_lines():
                    if line.startswith("data: "):
                        raw = line[6:].strip()
                        if not raw:
                            continue
                        try:
                            event = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        # 参考 V2 normalizeStreamEvent 规整化
                        normalized = normalize_stream_event(event)
                        yield normalized

                        # 终端状态退出
                        state = normalized.get("state", "")
                        if state in ("TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED"):
                            break

        except A2AError:
            raise
        except Exception as e:
            raise A2AError(f"A2A subscribe stream failed: {e}") from e
