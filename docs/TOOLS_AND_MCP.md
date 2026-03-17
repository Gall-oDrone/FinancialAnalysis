# ETL Tools and MCP Server

This document describes the **agent tools** built from the ETL-transform pipeline: how to use them with the **Claude (or OpenAI) Messages API** and how they are exposed as **MCP (Model Context Protocol) tools**.

---

## 1. Overview

The project exposes 14 tools in two ways:

| Usage | Description |
|-------|-------------|
| **Claude / OpenAI Messages API** | Pass `tools=get_tools_for_claude()` to `messages.create(...)`. When the model returns a `tool_use` block, call `run_tool(name, arguments)` and append the result as a `tool_result` message. |
| **MCP** | Run the MCP server with `python -m agents.mcp.server`. Any MCP client (e.g. Cursor, Claude Desktop, MCP Inspector) can list and call the same tools over stdio. |

All tools are implemented in `src/agents/tools/` and share the same schemas and handlers.

---

## 2. Tool List

### 2.1 Computation / utility (no side effects)

| Tool | Description |
|------|-------------|
| **extract_tickers** | Extract crypto/stock tickers from text (e.g. BTC-USD, (CRYPTO: BTC), $BTC, "bitcoin"). Input: `text` or `headline`/`summary`/`content`. |
| **analyze_sentiment** | Sentiment of text (positive/negative/neutral + scores). Input: `text`, optional `backend` (vader, textblob, transformers). |
| **extract_intent** | Classify financial news intent (market_update, regulatory_news, etc.). Input: `text`. |
| **extract_keywords** | Keywords and entities from text. Input: `text`, optional `top_n`, `method` (tfidf, spacy, rake). |
| **stock_risk_metrics** | Risk metrics from a price series: mean return, volatility, Sharpe, max drawdown, VaR/CVaR. Input: `prices` (list of numbers), optional `window`. |
| **build_s3_key_news** | Build S3 key for a news article or batch (per-article or run/week/month/year). Input: `article_id` + `datetime_str`, or `batch_type` + optional `agentic`. |
| **build_s3_key_stocks** | Build S3 key for transformed stocks (book + date). Input: `book`, `date`. |

### 2.2 Action / side-effect tools

| Tool | Description |
|------|-------------|
| **ingest_news** | Load raw financial news from PostgreSQL. Input: `date` or `since`/`until`. Returns row count and sample. |
| **ingest_stocks** | Load raw OHLCV from PostgreSQL. Input: `since`, optional `until`, `books`. |
| **run_news_transform** | Full news transform pipeline: ingest → sentiment/intent/keywords/tickers → optional save to Postgres. Input: `date` or `since`/`until`, `save_to_postgres`, `sentiment_backend`, `extract_tickers`. |
| **run_stocks_transform** | Full stocks transform: ingest → returns/volatility/indicators → optional save/upload. Input: `since`, `until`, `books`, `warmup_days`, `save_to_postgres`, `upload_s3`. |
| **enrich_article** | Enrich one article with LLM (summary + themes, or financial metrics). Input: `headline`, `content`/`summary`, optional `tickers`, `task` (summary_themes / financial_metrics). Requires LLM config. |
| **export_genai_jsonl** | Export transformed financial news from PostgreSQL to JSONL on S3 for GenAI/RAG. Input: optional `date` and `include_embeddings`. Writes to `genai/news/year=YYYY/month=MM/day=DD/format=jsonl/`. |

### 2.3 Batch / meta

| Tool | Description |
|------|-------------|
| **batch_tool** | Run multiple tools in one call. Input: `invocations` = list of `{ "name": "<tool_name>", "arguments": "<JSON string>" }`. |

---

## 3. Using tools with the Claude Messages API

1. **Get tool definitions**

   ```python
   from agents.tools import get_tools_for_claude

   tools = get_tools_for_claude()  # list of { name, description, input_schema }
   ```

2. **Call the API with `tools=`**

   ```python
   response = client.messages.create(
       model="claude-3-5-sonnet-20241022",
       max_tokens=1024,
       tools=tools,
       messages=[{"role": "user", "content": "What tickers are in this headline: Bitcoin soars as ETH-USD lags?"}],
   )
   ```

3. **Handle `tool_use` blocks**

   When `response.content` contains a block with `type="tool_use"`, run the tool and append the result:

   ```python
   from agents.tools import run_tool

   for block in response.content:
       if block.type == "tool_use":
           result = run_tool(block.name, block.input)
           # Append message: role="user", content=[{ type: "tool_result", tool_use_id: block.id, content: json.dumps(result) }]
   ```

4. **Execute a tool directly (no LLM)**

   ```python
   from agents.tools import run_tool

   out = run_tool("extract_tickers", {"text": "Bitcoin (CRYPTO: BTC) and ETH-USD rallied."})
   # out == {"tickers": ["BTC-USD", "ETH-USD"], "confidence": 0.95, "extraction_method": "pattern+name_mapping"}
   ```

---

## 4. MCP Server

### 4.1 Install MCP dependency

```bash
pip install -e ".[mcp]"
# or
pip install "mcp[cli]"
```

Requires **Python 3.10+**.

### 4.2 Run the server (stdio)

From the project root with `src` on `PYTHONPATH`:

```bash
PYTHONPATH=src python -m agents.mcp.server
```

Or after `pip install -e .`:

```bash
python -m agents.mcp.server
```

The server uses **stdio** transport: it reads JSON-RPC from stdin and writes responses to stdout. MCP clients (e.g. Cursor, Claude Desktop) typically start the server as a subprocess and communicate over stdio.

### 4.3 Configuring Cursor / Claude Desktop

Add the server to your MCP config (e.g. `~/.cursor/mcp.json` or the app’s MCP settings):

```json
{
  "mcpServers": {
    "financial-analysis-etl": {
      "command": "python",
      "args": ["-m", "agents.mcp.server"],
      "cwd": "/path/to/financial_analysis",
      "env": {
        "PYTHONPATH": "/path/to/financial_analysis/src"
      }
    }
  }
}
```

Ensure the `python` in `command` is the same environment where `mcp` and the project are installed.

### 4.4 Testing with MCP Inspector

1. Install Inspector: `npx -y @modelcontextprotocol/inspector`
2. Start our server (stdio) and connect the Inspector to it, or use a wrapper that exposes stdio as SSE/HTTP if your Inspector expects that.

---

## 5. Module layout

| Path | Purpose |
|------|---------|
| `src/agents/tools/schemas.py` | Tool names, descriptions, and JSON Schema `input_schema` for all tools. |
| `src/agents/tools/implementations.py` | Handler functions that take kwargs and return a JSON-serializable dict. |
| `src/agents/tools/__init__.py` | `get_tools_for_claude()`, `get_handler_by_name()`, `run_tool()`. |
| `src/agents/mcp/server.py` | MCP server: lists tools from schemas, executes via `run_tool()`, returns JSON text content. |

---

## 6. Dependencies

- **Core tools** (extract_tickers, analyze_sentiment, extract_intent, extract_keywords, stock_risk_metrics, build_s3_key_*): use only `transform.*` and `pipelines.etl_transform`; no DB or LLM required.
- **ingest_news / ingest_stocks**: require PostgreSQL and `storage.postgres` / `pipelines.etl_cli`.
- **run_news_transform / run_stocks_transform**: require DB and optionally S3 (if upload enabled).
- **enrich_article**: requires an LLM provider (OpenAI or Claude) configured via `agents.registry` and env (e.g. `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`).
- **MCP server**: requires the `mcp` package (`pip install "mcp[cli]"`).

---

## 7. References

- [Model Context Protocol](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [Claude Messages API – tool use](https://docs.anthropic.com/en/docs/build-with-claude/tool-use)
- Project doc: [AGENTIC_AI_AND_BRANCHING.md](./AGENTIC_AI_AND_BRANCHING.md)
