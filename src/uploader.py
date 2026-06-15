"""文件上传服务 — Hermes 版（参考 V2 files/upload-service.ts）"""

import os

import httpx


class A2AFileError(Exception):
    """文件操作异常"""

    def __init__(self, message: str):
        super().__init__(message)


# 允许的文件扩展名 → Content-Type
CONTENT_TYPES: dict[str, str] = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pdf": "application/pdf",
}


class UploadService:
    """文件上传服务（参考 V2 UploadService）

    小文件直接 POST /api/v1/files（FormData）
    大文件走预签名上传三步流程。
    """

    def __init__(self, rest_url: str, ensure_access_token):
        self._rest_url = rest_url.rstrip("/")
        self._ensure_access_token = ensure_access_token
        self._http = httpx.AsyncClient(timeout=120)
        # 上传策略（与 V2 一致）
        self._allowed_extensions = [".docx", ".pdf"]
        self._direct_upload_threshold = 5 * 1024 * 1024  # 5MB
        self._max_file_size = 25 * 1024 * 1024  # 25MB

    async def upload_attachment(self, filename: str, path: str) -> dict:
        """上传附件（参考 V2 uploadAttachment）"""
        # 1. 读取本地文件
        if not os.path.isfile(path):
            raise A2AFileError(f"文件不存在：{path}")
        size = os.path.getsize(path)
        ext = self._get_extension(filename)
        content_type = CONTENT_TYPES.get(ext, "application/octet-stream")

        # 2. 校验
        self._validate(filename, size)

        # 3. 选择上传路径
        if size <= self._direct_upload_threshold:
            return await self._upload_direct(path, filename, content_type)
        else:
            return await self._upload_presigned(path, filename, content_type, size)

    # ── 校验 ──────────────────────────────────────────────

    def _validate(self, filename: str, size: int) -> None:
        ext = self._get_extension(filename)
        if ext not in self._allowed_extensions:
            raise A2AFileError(
                f"不支持的文件类型：{ext or 'unknown'}。允许格式：{', '.join(self._allowed_extensions)}"
            )
        if size <= 0:
            raise A2AFileError("上传文件不能为空。")
        if size > self._max_file_size:
            raise A2AFileError(f"文件超过大小限制，当前上限为 {self._max_file_size} 字节。")

    @staticmethod
    def _get_extension(filename: str) -> str:
        idx = filename.lower().rfind(".")
        return filename[idx:] if idx != -1 else ""

    # ── 认证请求 ──────────────────────────────────────────

    async def _auth_request(self, method: str, path: str, **kwargs) -> dict:
        token = (await self._ensure_access_token()) or ""
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        url = f"{self._rest_url}{path}"

        resp = await self._http.request(method, url, headers=headers, **kwargs)
        if resp.status_code >= 400:
            body = await resp.aread()
            raise A2AFileError(body.decode("utf-8", errors="replace"))
        return resp.json() if resp.status_code != 204 else {}

    # ── 直接上传（小文件）─────────────────────────────────

    async def _upload_direct(self, path: str, filename: str, content_type: str) -> dict:
        with open(path, "rb") as f:
            files = {"file": (filename, f, content_type)}
            raw = await self._auth_request("POST", "/api/v1/files", files=files)
        return {
            "fileId": raw.get("fileId") or raw.get("file_id", ""),
            "filename": raw.get("filename", filename),
            "contentType": raw.get("contentType") or raw.get("content_type", content_type),
            "size": int(raw.get("size", 0)),
            "objectKey": raw.get("objectKey") or raw.get("object_key", ""),
        }

    # ── 预签名上传（大文件）───────────────────────────────

    async def _upload_presigned(
        self, path: str, filename: str, content_type: str, size: int
    ) -> dict:
        # 1. 获取预签名 URL
        presigned = await self._create_presigned(filename, content_type, size)
        upload_url = presigned.get("uploadUrl") or presigned.get("upload_url", "")
        object_key = presigned.get("objectKey") or presigned.get("object_key", "")
        if not upload_url or not object_key:
            raise A2AFileError("预签名上传响应格式异常")

        # 2. PUT 到预签名 URL
        with open(path, "rb") as f:
            data = f.read()
        await self._upload_to_presigned_url(upload_url, data, content_type)

        # 3. 确认上传
        confirmed = await self._confirm_presigned(object_key)

        return {
            "fileId": confirmed.get("fileId") or confirmed.get("file_id", ""),
            "filename": confirmed.get("filename", filename),
            "contentType": confirmed.get("contentType") or confirmed.get("content_type", content_type),
            "size": size,
            "objectKey": object_key,
        }

    async def _create_presigned(self, filename: str, content_type: str, size: int) -> dict:
        return await self._auth_request(
            "POST",
            "/api/v1/files/presigned-upload",
            json={"filename": filename, "content_type": content_type, "size": size},
        )

    async def _upload_to_presigned_url(self, upload_url: str, data: bytes, content_type: str) -> None:
        resp = await self._http.put(
            upload_url,
            content=data,
            headers={"Content-Type": content_type},
        )
        if resp.status_code >= 400:
            body = await resp.aread()
            raise A2AFileError(f"预签名上传失败：{body.decode('utf-8', errors='replace')}")

    async def _confirm_presigned(self, object_key: str) -> dict:
        return await self._auth_request(
            "POST",
            "/api/v1/files/presigned-upload/confirm",
            json={"object_key": object_key},
        )
