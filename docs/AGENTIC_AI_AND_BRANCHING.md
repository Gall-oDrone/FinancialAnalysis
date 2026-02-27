# Agentic AI in ETL Transforms & Branching Recommendation

## Summary

Agentic AI (Claude API, OpenAI API, etc.) is implemented in the ETL transform layer using **OOP** and a **production-ready** approach. New modules for **RAG** (vector stores, chunking) and **Model Context Protocol (MCP)** are in place. This document describes the design and whether to use a **separate GitHub branch** for these changes.

---

## 1. How Agentic AI Is Implemented in ETL-Transform Tasks

### OOP design

- **`LLMClient` (abstract)**  
  All providers implement the same interface: `complete(messages)`, `complete_text(prompt)`, plus `provider_name` and `model_id`. ETL code depends only on this interface.

- **Concrete clients**  
  - `OpenAIClient` (OpenAI API)  
  - `ClaudeClient` (Anthropic API)  
  Additional providers (e.g. Bedrock, Azure OpenAI) can be added by implementing `LLMClient` and registering in `src/agents/registry.py`.

- **Configuration**  
  - `agents.base.AgentConfig`: dataclass used by clients (timeout, retries, temperature, etc.).  
  - `config.settings.LLMAgentConfig`: loads from env (`LLM_PROVIDER`, `LLM_MODEL`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc.).  
  No API keys in code; use environment or `.env`.

- **Pipeline integration**  
  - New stage: `PipelineStage.AGENTIC_TRANSFORM`.  
  - `AgenticTransformStageHandler` runs **after** the existing text transform.  
  - It uses `get_llm_client(provider)` and `AgenticTextEnricher` to add columns (e.g. `llm_summary`, `llm_themes`).  
  - Enable by setting `enable_agentic_transform=True` on `PipelineConfig` or `LLM_ENABLE_AGENTIC_TRANSFORM=true` in env.  
  - Optional `agentic_transform_max_rows` caps how many rows are sent to the LLM (useful for testing).

- **Production-oriented behavior**  
  Retries with backoff, timeout, per-row error handling (skip or raise), structured logging, and optional rate limiting (config only; implementation can be added later).

### Where it lives

| Area | Path |
|------|------|
| Base types & config | `src/agents/base.py` |
| Client registry | `src/agents/registry.py` |
| OpenAI / Claude | `src/agents/clients/openai_client.py`, `claude_client.py` |
| Agentic transform | `src/agents/transforms/agentic_transform.py` |
| Pipeline stage | `src/pipelines/pipeline.py` (`AgenticTransformStageHandler`, `AGENTIC_TRANSFORM` stage) |
| App config | `src/config/settings.py` (`LLMAgentConfig`) |

---

## 2. RAG Module

- **Interfaces** in `src/rag/base.py`: `VectorStore`, `DocumentChunk`, `RetrieverResult`.  
- **Chunking** in `src/rag/chunking.py`: `TextChunker` (by size/overlap, from text or DataFrame).  
- **Store** in `src/rag/stores/memory.py`: `InMemoryVectorStore` (cosine similarity, optional embedding function for `search_by_text`).  
- Use with existing GenAI export: chunk the transformed/exported articles, optionally use the same embedding model as in `genai_export`, and add chunks to the store for retrieval in agents or MCP.

---

## 3. Model Context Protocol (MCP)

- **Placeholder** in `src/agents/mcp/`: package exists for future MCP server and tools.  
- Intended use: expose pipeline results, RAG retriever, and GenAI export metadata as MCP resources/tools so external MCP clients can use them.  
- No concrete MCP server implementation yet; add when you integrate with an MCP SDK.

---

## 4. Separate Branch vs. ETL-Transforms

**Recommendation: use a separate feature branch for this work.**

| Approach | Pros | Cons |
|----------|------|------|
| **Separate branch** (e.g. `feature/agentic-ai` or `etl-transforms/agentic-ai`) | Clear review surface; ETL-transforms stays stable; easy to merge when ready; CI can run with optional LLM deps. | One more branch to keep in sync. |
| **Everything on `etl-transforms`** | Single branch; simpler if ETL-transforms is already the main development branch. | Mixes “classic” ETL with new AI features; harder to roll back or disable agentic/MCP if needed. |

**Suggested workflow**

1. Create a branch from `etl-transforms` (or from `main` if ETL-transforms is merged):  
   `feature/agentic-ai` or `etl-transforms/agentic-ai`.
2. Keep all Agentic AI, RAG, and MCP changes on that branch until reviewed and tested.
3. Merge into `etl-transforms` (or `main`) when you’re satisfied.  
   Agentic transform is **opt-in** via config, so merging does not change default ETL behavior.

**If you prefer a single branch**

- Keeping everything on `etl-transforms` is acceptable **if** `etl-transforms` is already the long-lived feature branch for ETL and you’re okay with agentic/RAG/MCP living there.  
- Keep the new code in dedicated modules (`agents/`, `rag/`, `agents/mcp/`) and guard agentic usage with `enable_agentic_transform` and optional dependencies so the core ETL path stays unchanged by default.

---

## 5. Optional Dependencies

Install only the provider you use:

```bash
# OpenAI (for OpenAIClient)
pip install openai

# Anthropic (for ClaudeClient)
pip install anthropic
```

Add to `requirements.txt` or `pyproject.toml` as optional extras (e.g. `[agents]` or `[llm]`) so CI and minimal installs don’t require them.

---

## 6. Quick Start

- **Enable agentic transform in pipeline**  
  Set `PipelineConfig(..., enable_agentic_transform=True)` or `LLM_ENABLE_AGENTIC_TRANSFORM=true`, and set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`.

- **Use RAG**  
  Chunk your GenAI/transformed DataFrame with `TextChunker.chunk_dataframe`, optionally generate embeddings (e.g. with your existing sentence-transformers setup), then `InMemoryVectorStore.add(chunks)` and `search_by_text(...)` or `search(query_embedding, top_k=5)`.

- **MCP**  
  Implement an MCP server in `src/agents/mcp/` that uses the pipeline and RAG interfaces when you’re ready to adopt an MCP SDK.
