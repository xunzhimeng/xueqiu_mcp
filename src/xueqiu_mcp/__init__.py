"""Xueqiu MCP - 基于雪球API的MCP服务"""

from xueqiu_mcp.server import mcp

__version__ = "0.2.0"


def main():
    """MCP 服务入口函数"""
    mcp.run()
