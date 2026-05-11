# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rbot is a Python crypto trading bot framework with a Rust backend (exposed via PyO3/maturin). It supports three execution modes — `backtest`, `dry_run`, and `production` — without requiring agent code changes.

## Build and Development Commands

```bash
# Activate virtual environment first
source .venv/bin/activate

# Build and install Python extension (development)
maturin develop
# or
.venv/bin/python3 -m pip install .

# Build for release
maturin build --release

# Run Rust unit tests
cargo test

# Run a single Rust test
cargo test test_name

# Run all Python integration tests
pytest test/

# Run a specific Python test file
pytest test/00_suite/test_01market_basic.py

# Run a specific Python test function
pytest test/00_suite/test_01market_basic.py::test_function_name -v
```

## Environment Setup

API keys are read from `~/.rusty-bot/.env`. Key format:
- Production: `{EXCHANGE_NAME}_API_KEY`, `{EXCHANGE_NAME}_API_SECRET`
- Test/sandbox: `{EXCHANGE_NAME}_API_KEY_TEST`, `{EXCHANGE_NAME}_API_SECRET_TEST`

Database root defaults to the user's home directory; override with `RBOT_DB_ROOT` environment variable.

## Architecture

### PyO3 Bridge

The root crate (`src/lib.rs`) is a `cdylib` that assembles the Python module `rbot` from all workspace crates. All `#[pyclass]` and `#[pymethods]` structs/functions in workspace crates are re-exported here. The build tool is maturin.

### Workspace Modules

- **`modules/rbot_lib`** — foundational layer: `common/` (types: `Order`, `Trade`, `MarketConfig`, `ExchangeConfig`, `OrderBook`, time utils), `db/` (hybrid Parquet + SQLite storage via `TradeDataFrame`/`TradeDb`), `net/` (REST, WebSocket, UDP helpers)
- **`modules/rbot_market`** — `MarketImpl` and `OrderInterfaceImpl` traits that every exchange market struct must implement
- **`modules/rbot_session`** — `Session` (the object the agent receives in callbacks), `Runner` (drives backtest/dry/real loops), `Logger` (persists results as Polars DataFrames), `OrderList`
- **`modules/rbot_server`** — order book broadcast server (actix-web + UDP multicast via `MARKET_HUB`)
- **`modules/rbot_blockon`** — `BLOCK_ON` macro for executing async Rust from synchronous PyO3 call sites

### Exchange Modules (`exchanges/`)

Each exchange directory follows this file pattern:
- `market.rs` — `#[pyclass]` struct (e.g. `Bitbank`) exposed to Python; implements `MarketImpl` + `OrderInterfaceImpl`
- `config.rs` — exchange/market config constants (e.g. `BitbankConfig::BTCJPY`)
- `message.rs` — serde structs for REST and WebSocket JSON payloads
- `rest.rs` — REST API client (`BitbankRestApi`)
- `ws.rs` — WebSocket clients for public and private streams

Active exchanges: `bybit`, `binance`, `bitbank`. `bitflyer`, `hyperliquid`, `ccxt`, `example` exist but are disabled/incomplete in the workspace.

### Agent Callback Pattern

Agents are Python classes with any subset of these methods:

```python
def on_init(self, session): ...      # called once at start; set session.clock_interval_sec here
def on_tick(self, session, side, price, size): ...   # every trade tick
def on_clock(self, session, clock): ...              # periodic clock (microsecond timestamp)
def on_update(self, session, updated_order): ...     # order status change
```

`Runner` inspects the agent with `has_method()` at startup and only calls methods that exist.

### Data Storage

- Historical tick data: Parquet files partitioned by day (`TradeDataFrame`)
- Intraday/recent data: SQLite (`TradeDb`)
- Session results: Polars `DataFrame` via `Logger`; can be saved/restored from `.log` files

### Key Internal Mechanism

`MARKET_HUB` is a global channel hub (UDP multicast, port 3001 by default) used to fan out `MarketMessage` events (trades, order book updates, order fills) between the exchange WebSocket threads and the `Runner` event loop.

## Testing Structure

- `test/00_suite/` — integration tests that run against real or mocked exchanges
- `test/{exchange}_test/` — per-exchange scripts and notebooks
- `test/manual/` — exploratory Jupyter notebooks (not part of automated suite)
