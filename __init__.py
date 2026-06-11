"""Hermes 插件 — 合同审核（入口 + 工具注册）"""

from .src.runtime import PluginRuntime

svc = None


def register(ctx):
    global svc
    svc = PluginRuntime(ctx)
