"""通知器 — Hermes 版（全异步实现）"""

import asyncio
import json
import random
import threading
import time
from datetime import datetime
from abc import ABC, abstractmethod

import httpx


class BaseNotifier(ABC):
    """通知器基类 — 仅抽象接口"""

    @abstractmethod
    async def send_text(self, target: str, text: str) -> bool:
        ...

    @abstractmethod
    def get_last_error(self) -> str | None:
        ...


class SafeNotifier(BaseNotifier):
    """带超时保护的异步通知器"""

    def __init__(self, timeout_ms: int = 30_000):
        self._last_error: str | None = None
        self._timeout_ms = timeout_ms

    def get_last_error(self) -> str | None:
        return self._last_error

    async def send_text(self, target: str, text: str) -> bool:
        self._last_error = None
        try:
            return await asyncio.wait_for(
                self._do_send(target, text),
                timeout=self._timeout_ms / 1000,
            )
        except asyncio.TimeoutError:
            self._errors.append("sendMessage timed out")
            return False
        except Exception as e:
            self._errors.append(str(e))
            return False

    @abstractmethod
    async def _do_send(self, target: str, text: str) -> bool:
        ...


class FeishuNotifier(SafeNotifier):
    """飞书消息通知器（异步 httpx）"""

    FEISHU_API = "https://open.feishu.cn"
    LARK_API = "https://open.larksuite.com"

    def __init__(self, app_id: str, app_secret: str, domain: str = "feishu"):
        super().__init__()
        self._http = httpx.AsyncClient(timeout=30)
        self.app_id = app_id
        self.app_secret = app_secret
        self._base_url = self.LARK_API if "lark" in domain.lower() else self.FEISHU_API
        self._cached_token: str | None = None
        self._token_expires_at: float = 0

    def _ensure_token(self) -> str | None:
        now = time.time()
        if self._cached_token and now < self._token_expires_at:
            return self._cached_token
        return None  # 标记需要刷新

    async def _refresh_token(self) -> str | None:
        try:
            url = f"{self._base_url}/open-apis/auth/v3/tenant_access_token/internal"
            resp = await self._http.post(url, json={
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            })
            resp.raise_for_status()
            body = resp.json()

            if body.get("code") != 0:
                self._last_error = f"Feishu token API error: code={body.get('code')}"
                return None

            expire = body.get("expire", 7200)
            self._token_expires_at = time.time() + max(expire - 300, 60)
            self._cached_token = body.get("tenant_access_token")
            return self._cached_token
        except Exception as e:
            self._last_error = f"Feishu token refresh failed: {e}"
            return None

    async def _do_send(self, target: str, text: str) -> bool:
        token = self._ensure_token()
        if token is None:
            token = await self._refresh_token()
        if not token:
            return False

        try:
            url = f"{self._base_url}/open-apis/im/v1/messages?receive_id_type=open_id"
            resp = await self._http.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "receive_id": target,
                    "msg_type": "text",
                    "content": json.dumps({"text": text}),
                },
            )
            if not resp.is_success:
                detail = resp.text[:500]
                self._last_error = f"Feishu sendText failed: HTTP {resp.status_code} {detail}"
                return False
            return True
        except Exception as e:
            self._last_error = f"Feishu sendText failed: {e}"
            return False


class WechatNotifier(BaseNotifier):
    """微信通知器 — 消息缓冲 + 5 秒一次异步发送"""

    TIMEOUT = 20

    def __init__(self, send_fn):
        self.send_fn = send_fn
        self._buffer: list[str] = []
        self._send_task: asyncio.Task | None = None
        self._errors: list[str] = []

    def get_last_error(self) -> str | None:
        if not self._errors:
            return None
        return "\n".join(self._errors)

    @staticmethod
    def _fmt_list(text: str) -> str:
        """将文本转为 markdown 列表：每行前加 `- `"""
        return "- " + text.replace("\n", "\n- ")

    async def send_text(self, target: str, text: str) -> bool:
        """追加到缓冲区，根据错误列表判断是否成功"""
        self._buffer.append(self._fmt_list(text))
        if self._send_task is None or self._send_task.done():
            self._send_task = asyncio.create_task(self._send_loop(target))
        return len(self._errors) == 0

    def _parse_result(self, result) -> str | None:
        if isinstance(result, dict):
            return result.get("error")
        if isinstance(result, str):
            import json
            try:
                parsed = json.loads(result)
                return parsed.get("error") if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                pass
        return None

    async def _send_loop(self, target: str):
        """后台循环：持续运行，有消息则 15 秒间隔发送，无消息时空转"""
        while True:
            while self._buffer:
                entry = self._buffer.pop(0)
                if not entry or not entry.strip():
                    continue
                try:
                    ts = datetime.now().strftime("%H:%M:%S")
                    result = await asyncio.wait_for(
                        # self.send_fn(target, f"[{ts}] {entry}"),
                        self.send_fn(target, entry),
                        timeout=self.TIMEOUT,
                    )
                    err = self._parse_result(result)
                    if err is not None:
                        self._errors.append(err)
                except asyncio.TimeoutError:
                    self._errors.append("sendMessage timed out")
                except Exception as e:
                    self._errors.append(str(e))
                await asyncio.sleep(15 + random.randint(10, 60) / 10)
            await asyncio.sleep(2)


class AgentNotifier(BaseNotifier):
    """Agent 通知器 — 消息存储在内存缓冲区，供主线程读取

    - 写（插件协程线程）：send_text 追加消息到缓冲区
    - 读（Hermes 主线程）：read_and_clear 一次全部读出并清空
    """

    def __init__(self):
        super().__init__()
        self._messages: list[str] = []
        self._seq: int = 0
        self._last_text: str = ""
        self._lock = threading.Lock()

    def get_last_error(self) -> str | None:
        return None

    async def send_text(self, target: str, text: str) -> bool:
        if text == self._last_text:
            return True
        self._last_text = text
        with self._lock:
            self._seq += 1
            self._messages.append(f"[{self._seq}] {text}")
        return True

    def read_and_clear(self) -> str:
        with self._lock:
            text = "\n".join(self._messages)
            self._messages.clear()
        return text
