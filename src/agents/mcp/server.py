"""
MCP server exposing ETL and transform tools.

Run with: python -m agents.mcp.server (or uv run/python with mcp dependency installed).
Uses stdio transport by default; requires Python 3.10+ and mcp package.
"""

import json
import sys
from typing import Any, Dict

# Lazy import so we can document and test without mcp installed
def _get_server():
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp import types

    from agents.tools import get_all_schemas, run_tool

    def _make_tool(schema: Dict[str, Any]) -> types.Tool:
        return types.Tool(
            name=schema["name"],
            description=schema.get("description", ""),
            inputSchema=schema.get("input_schema", {"type": "object", "properties": {}}),
        )

    async def handle_list_tools(
        ctx: Any,
        params: types.PaginatedRequestParams | None,
    ) -> types.ListToolsResult:
        tools = [_make_tool(s) for s in get_all_schemas()]
        return types.ListToolsResult(tools=tools)

    async def handle_call_tool(
        ctx: Any,
        params: types.CallToolRequestParams,
    ) -> types.CallToolResult:
        name = params.name
        arguments = params.arguments or {}
        try:
            result = run_tool(name, arguments)
            text = json.dumps(result, indent=2)
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=text)],
                isError=False,
            )
        except Exception as e:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps({"error": str(e)}))],
                isError=True,
            )

    app = Server(
        "financial-analysis-etl",
        on_list_tools=handle_list_tools,
        on_call_tool=handle_call_tool,
    )
    return app, stdio_server


def main() -> int:
    """Run the MCP server (stdio)."""
    try:
        app, transport = _get_server()
    except ImportError as e:
        print("MCP server requires the mcp package. Install with: pip install 'mcp[cli]'", file=sys.stderr)
        print(str(e), file=sys.stderr)
        return 1

    import anyio

    async def run():
        async with transport() as (read, write):
            await app.run(read, write, app.create_initialization_options())

    anyio.run(run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
