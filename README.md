# IGS MCP Server (Rust)

Intelligence Gathering System - Rust implementation using [rmcp](https://crates.io/crates/rmcp) (modelcontextprotocol/rust-sdk) and [TOON](https://lib.rs/crates/toon-format) for token-efficient AI agent output.

## Overview

IGS MCP monitors intelligence from 223+ curated RSS/HTTP sources across global news, geopolitics, tech, research, and regional topics. Built in Rust for performance and low memory footprint (~14 MB binary, ~5 MB RSS).

### Features

| Domain       | Tools                                                                                | What it does                                                                                                                                      |
| ------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **News**     | `news.fetch`, `news.testSource`, `news.enrich`                                       | Fetch from 200+ RSS/HTML/JSON sources with pool/country/city/domain/keyword/time filtering. Offline NLP enrichment (topics, entities, sentiment). |
| **Pools**    | `pools.list`, `pools.upsert`, `pools.delete`                                         | Manage monitoring pool categories                                                                                                                 |
| **Sources**  | `sources.list/upsert/delete`, `sources.autodiscover`, `sources.enableGenericScraper` | Manage RSS/HTTP sources with autodiscovery                                                                                                        |
| **Geo**      | `sources.countries`, `sources.cities`, `sources.domains`                             | List countries/cities/domains with available source counts                                                                                        |
| **Parsers**  | `parsers.list`                                                                       | List available parser ids                                                                                                                         |
| **Reddit**   | `reddit.search`                                                                      | Search Reddit posts and comments                                                                                                                  |
| **Research** | `research.search`, `research.paper`, `research.download`                             | Search arXiv + Semantic Scholar, get paper details, download PDFs                                                                                 |
| **Web**      | `web.search`, `web.scrape`, `web.map`                                                | Web search (Tavily/Firecrawl), scrape, sitemap discovery                                                                                          |
| **Insights** | `insights.*` (6 tools)                                                               | Cross-domain entity connection engine, trending detection                                                                                         |

### Token-Efficient Output

Large data sets are formatted using [TOON (Token-Oriented Object Notation)](https://lib.rs/crates/toon-format), a compact alternative to JSON that reduces token usage for AI agent consumption by ~40–60%.

## Quick Start

### Prerequisites

- Rust 1.75+
- (Optional) Tavily or Firecrawl API keys for web search/scrape

### Build & Run

```bash
cargo build --release
./target/release/igs-mcp
```

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "igs-mcp": {
      "command": "/absolute/path/to/igs-rust-mcp/target/release/igs-mcp"
    }
  }
}
```

### Environment Variables

| Variable         | Default              | Description                        |
| ---------------- | -------------------- | ---------------------------------- |
| `IGS_CONFIG_DIR` | `~/.config/igs-mcp/` | Config directory override          |
| `RUST_LOG`       | `info`               | Log level (e.g., `debug`, `trace`) |

### Config Files

Config YAML files are auto-bootstrapped from `./config/` to `~/.config/igs-mcp/` on first run:

| File            | Purpose                                       |
| --------------- | --------------------------------------------- |
| `settings.yml`  | HTTP client, cache, Tavily/Firecrawl API keys |
| `pools.yml`     | Pool definitions and categories               |
| `sources.yml`   | 223+ RSS/HTTP/JSON source definitions         |
| `countries.yml` | Country metadata for geo-filtering            |

### Web Search Setup

Edit `~/.config/igs-mcp/settings.yml` to add API keys:

```yaml
tavily:
  enabled: true
  api_key: "<YOUR_TAVILY_API_KEY>"

firecrawl:
  enabled: true
  api_key: "<YOUR_FIRECRAWL_API_KEY>"
```

## Architecture

```
src/
├── main.rs      ── Entry point (tokio + rmcp stdio transport)
├── lib.rs       ── Module declarations
├── server.rs    ── MCP server handler + 29 #[tool] methods + TOON helpers
├── config.rs    ── YAML config loading (pools, sources, settings, countries)
├── types.rs     ── Shared types (Pool, Source, NewsItem, ResearchPaper, etc.)
├── http.rs      ── HTTP client with retry + semaphore concurrency + feed cache
├── cache.rs     ── File-based feed cache + query cache with TTL
└── parsers.rs   ── RSS/Atom, JSON Feed, generic HTML parsers + keyword/time filters
```

## License

MIT
