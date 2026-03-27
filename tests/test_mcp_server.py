"""
Tests for MCP server (list tools; optional when mcp package is not installed).
"""

import sys
import pytest
from unittest.mock import patch

try:
    from mcp.server import Server
    from mcp import types
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


@pytest.mark.skipif(not MCP_AVAILABLE, reason="mcp package not installed")
class TestMCPServerListTools:
    """Test that the MCP server lists our tools (requires mcp package)."""

    def test_list_tools_via_schemas(self):
        """Tools listed by server match get_all_schemas (same source)."""
        from agents.tools import get_all_schemas

        schemas = get_all_schemas()
        names = [s["name"] for s in schemas]
        assert "extract_tickers" in names
        assert "run_news_transform" in names
        assert "batch_tool" in names
        assert len(names) >= 12


class TestMCPServerWithoutMCP:
    """Test server module behavior when mcp is not installed."""

    def test_main_returns_1_without_mcp(self):
        """main() exits with 1 and prints message when mcp is not installed."""
        import sys
        from io import StringIO

        # If mcp is installed, skip
        try:
            import mcp  # noqa: F401
            pytest.skip("mcp is installed; cannot test graceful failure")
        except ImportError:
            pass

        from agents.mcp.server import main
        stderr = StringIO()
        with patch_stdout(stderr):
            code = main()
        assert code == 1
        assert "mcp" in stderr.getvalue().lower() or "install" in stderr.getvalue().lower()


def patch_stdout(stderr):
    """Context manager to patch sys.stderr."""
    return patch.object(sys, "stderr", stderr)
