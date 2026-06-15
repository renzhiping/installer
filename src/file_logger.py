"""文件日志系统 — Hermes 版（参考 V2 FileLogger）"""

import os
from datetime import datetime


class FileLogger:
    def __init__(self, base_dir: str | None = None):
        if base_dir is None:
            base_dir = os.path.expanduser("~/.hermes/logs/contract_review")
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        log_dir = base_dir
        os.makedirs(log_dir, exist_ok=True)
        self._path = os.path.join(log_dir, f"{ts}.log")

    def info(self, msg: str) -> None:
        self._write("INFO", msg)

    def warn(self, msg: str) -> None:
        self._write("WARN", msg)

    def error(self, msg: str) -> None:
        self._write("ERROR", msg)

    def _write(self, level: str, msg: str) -> None:
        line = f"[{datetime.now().isoformat()}] [{level}] {msg}\n"
        try:
            with open(self._path, "a") as f:
                f.write(line)
        except OSError:
            pass
