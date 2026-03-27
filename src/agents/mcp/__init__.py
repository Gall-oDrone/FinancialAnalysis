"""
Model Context Protocol (MCP) integration points.

MCP is a standard for exposing tools and resources to AI models. This package
provides adapters to expose pipeline outputs, RAG retrievers, and config as
MCP resources/tools so that external MCP servers or clients can consume them.

Future: implement MCP server that serves:
- Pipeline status and last run results
- RAG vector store as a retrieval tool
- GenAI export paths and sample documents

See: https://modelcontextprotocol.io
"""

# MCP server: run with python -m agents.mcp.server (requires pip install 'mcp[cli]')
# from agents.mcp.server import main  # stdio server entrypoint

__all__: list = ["server"]
