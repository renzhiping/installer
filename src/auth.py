"""认证模块 — Hermes 版（参考 V2 AuthFacade + SessionManager + AuthService）"""

import asyncio
import datetime
import json
import time
from urllib.parse import urljoin

import httpx


# ── SessionManager ──────────────────────────────────────

class SessionManager:
    """认证状态管理"""

    def __init__(self):
        self.status: str = "logged_out"
        self.session: dict | None = None
        self.token_bundle: dict | None = None
        self.binding: dict | None = None

    def get_status(self) -> str:
        return self.status

    def get_state(self) -> dict:
        return {
            "status": self.status,
            "session": dict(self.session) if self.session else None,
            "tokenBundle": dict(self.token_bundle) if self.token_bundle else None,
            "binding": dict(self.binding) if self.binding else None,
        }

    def get_access_token(self) -> str | None:
        return self.token_bundle.get("accessToken") if self.token_bundle else None

    def get_refresh_token(self) -> str | None:
        return self.token_bundle.get("refreshToken") if self.token_bundle else None

    def is_access_token_expiring_within(self, window_ms: int = 30_000) -> bool:
        access_token = self.get_access_token()
        if not access_token:
            return True
        expires_at = self.token_bundle.get("expiresAt") if self.token_bundle else None
        if not expires_at:
            return False
        try:
            exp = datetime.datetime.fromisoformat(expires_at).timestamp() * 1000
        except (ValueError, TypeError):
            return False
        return exp - time.time() * 1000 <= window_ms

    def set_session(self, s: dict) -> None:
        self.status = "session_pending"
        self.session = s

    def set_tokens(self, bundle: dict) -> None:
        self.token_bundle = bundle

    def set_binding(self, binding: dict) -> None:
        self.binding = binding
        self.status = "ready"

    def clear(self) -> None:
        self.status = "logged_out"
        self.session = None
        self.token_bundle = None
        self.binding = None


# ── AuthFacade ──────────────────────────────────────────

class AuthFacade:
    """认证门面（参考 V2 AuthFacade）"""

    def __init__(self, auth_base_url: str | None = None, http_client=None):
        self._auth_base_url = auth_base_url.rstrip("/") if auth_base_url else None
        self._http = http_client
        self._session_manager = SessionManager()
        self._refresh_in_flight = None

    def _api_url(self, path: str) -> str:
        return urljoin(f"{self._auth_base_url}/", path.lstrip("/")) if self._auth_base_url else ""

    def ping_auth(self) -> str:
        """检查认证服务是否可达。成功返回空字符串，失败返回错误描述。"""
        if not self._auth_base_url:
            return "authBaseUrl 未配置"
        try:
            httpx.get(self._auth_base_url, timeout=5)
            return ""
        except Exception as e:
            return f"认证服务不可达，请检查网络或 authBaseUrl 配置（{self._auth_base_url}）: {e}"

    async def ensure_access_token(self) -> str | None:
        sm = self._session_manager
        if not sm.is_access_token_expiring_within(30_000):
            return sm.get_access_token()
        rt = sm.get_refresh_token()
        if not rt:
            return None
        if self._refresh_in_flight:
            return self._refresh_in_flight
        return await self._do_refresh(rt)

    async def _do_refresh(self, refresh_token: str) -> str | None:
        try:
            return await self._refresh_token(refresh_token)
        except Exception:
            self._session_manager.clear()
            return None

    async def login(self, context: dict, on_notifier=None, on_result=None, cancel=None):
        """登录流程（SSE 流式，参考 V2 createSessionAndWait）"""
        http = self._http or httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=15.0))
        pending_session_id = None
        pending_user_code = None

        try:
            # 1. POST 创建 session，读取 SSE 流
            url = self._api_url("api/auth/sessions")

            async with http.stream("POST", url, json={
                "im_platform": context.get("imPlatform", ""),
                "im_user_id": context.get("imUserId", ""),
                "channel_id": context.get("channelId", ""),
                "thread_id": context.get("threadId", ""),
            }) as resp:
                sse_event_type = ""
                async for line in resp.aiter_lines():
                    if cancel and cancel.is_set():
                        return
                    if line.startswith("event: "):
                        sse_event_type = line[7:].strip()
                        continue
                    if not line.startswith("data: "):
                        continue
                    raw = line[6:].strip()
                    if not raw:
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    event_type = sse_event_type or event.get("type", "")
                    sse_event_type = ""

                    if event_type == "pending":
                        pending_session_id = event.get("sessionId") or event.get("session_id")
                        pending_user_code = event.get("code") or event.get("user_code")
                        login_url = event.get("loginUrl") or event.get("login_url")

                        self._session_manager.set_session({
                            "sessionId": pending_session_id,
                            "loginUrl": login_url,
                            "userCode": pending_user_code,
                            "status": "pending",
                        })

                        if on_notifier:
                            result = on_notifier(login_url, pending_user_code)
                            await result
                                

                    elif event_type == "approved":
                        break

                    elif event_type == "expired":
                        if on_result:
                            await on_result({"ok": False, "error": f"认证已过期：{event.get('reason', 'session expired')}"})
                        return

                else:
                    # 流正常结束但没有 approved
                    if on_result:
                        await on_result({"ok": False, "error": "SSE 流意外结束"})
                    return

            # 2. 交换 token（字段名 snake_case，参考 V1 exchangeToken）
            token_url = self._api_url("api/auth/token")
            token_resp = await http.post(token_url, json={
                "session_id": pending_session_id,
                "user_code": pending_user_code,
            })
            token_resp.raise_for_status()
            token_body = token_resp.json()
            token_bundle = {
                "accessToken": token_body.get("accessToken") or token_body.get("access_token", ""),
                "refreshToken": token_body.get("refreshToken") or token_body.get("refresh_token"),
                "expiresAt": token_body.get("expiresAt") or token_body.get("expires_at"),
            }
            self._session_manager.set_tokens(token_bundle)

            # 4. 绑定身份（snake_case，参考 V1 toBindIdentityBody）
            bind_url = self._api_url("api/auth/bindings")
            bind_resp = await http.post(bind_url, json={
                "session_id": pending_session_id,
                "im_platform": context.get("imPlatform", ""),
                "im_user_id": context.get("imUserId", ""),
            }, headers={"Authorization": f"Bearer {token_bundle['accessToken']}"})
            bind_resp.raise_for_status()
            bind_body = bind_resp.json()
            self._session_manager.set_binding({
                "imPlatform": bind_body.get("imPlatform") or bind_body.get("im_platform", ""),
                "imUserId": bind_body.get("imUserId") or bind_body.get("im_user_id", ""),
                "channelId": bind_body.get("channelId") or bind_body.get("channel_id", ""),
                "threadId": bind_body.get("threadId") or bind_body.get("thread_id", ""),
                "status": "bound",
            })

            if on_result:
                await on_result({"ok": True})

        except Exception as exc:
            ping = self.ping_auth()
            msg = str(exc).strip()
            reason = f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__
            err = f"{reason}（{ping}）" if ping else reason
            if on_result:
                await on_result({"ok": False, "error": err})

    async def _refresh_token(self, refresh_token: str) -> str | None:
        http = self._http or httpx.AsyncClient(timeout=30)
        url = self._api_url("api/auth/token/refresh")
        resp = await http.post(url, json={"refreshToken": refresh_token})
        resp.raise_for_status()
        body = resp.json()
        bundle = {
            "accessToken": body.get("accessToken") or body.get("access_token", ""),
            "refreshToken": body.get("refreshToken") or body.get("refresh_token"),
            "expiresAt": body.get("expiresAt") or body.get("expires_at"),
        }
        self._session_manager.set_tokens(bundle)
        return bundle["accessToken"]

    def is_logged_in(self) -> bool:
        state = self._session_manager.get_state()
        return state["status"] == "ready" and bool(state.get("tokenBundle", {}).get("accessToken"))

    def clear(self) -> None:
        self._session_manager.clear()
