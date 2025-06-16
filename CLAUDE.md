# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Update CLAUDE.md

This file should be updated periodically to reflect the current state of the project. If you make changes to the codebase, please update this file accordingly.

## Project Overview

rbot is a Python crypto trading bot framework written in Rust, designed for tick-based backtesting and live trading. The core architecture includes:

- **Exchange modules** (`exchanges/`) - Exchange-specific implementations (Binance, Bybit, Bitbank, Bitflyer)
- **Core library** (`modules/rbot_lib/`) - Common utilities, database, networking components
- **Session management** (`modules/rbot_session/`) - Trading session, order management, and Python interface
- **Market data** (`modules/rbot_market/`) - Market data handling and abstraction
- **Server components** (`modules/rbot_server/`) - Server functionality

The framework supports three execution modes: backtest, dry_run, and production, allowing bots to run without modification across different environments.

## Build and Development Commands

### Rust Development
```bash
`.venv/bin/python3 -m pip install .`

# Run tests
cargo test

# Build Python extension
maturin develop

# Build for release
maturin build --release
```

### Python Testing
```bash
# Run Python tests (uses pytest)
pytest test/

# Run specific test suite
pytest test/00_suite/

# Run with verbose output
pytest -v test/
```

### Environment Setup
- Exchange API keys are configured in `~/.rusty-bot/.env` file
- Database location can be overridden with `RBOT_DB_ROOT` environment variable
- API key is stored in `{EXCHANGE_NAME}_API_KEY`
- API secret is stored in `{EXCHANGE_NAME}_API_SECRET`

## Architecture Notes

### Module Dependencies
- `rbot_lib` contains foundational components (database, networking, common utilities)
- `rbot_session` handles trading logic and Python bindings using PyO3
- Exchange modules (`exchanges/`) implement exchange-specific APIs and WebSocket handling
- The project uses a workspace structure with shared dependencies

### Key Components
- **Session**: Provides abstraction layer for market interaction, ordering, and data access
- **Market**: API and database wrapper for specific trading pairs per exchange
- **Runner**: Orchestrates session execution across different modes (backtest/dry_run/production)
- **Logger**: Stores execution results in Polars DataFrames for analysis

### Data Storage
- Old Historical data stored in Parquet format (changed from SQLite in v0.4.0)
- Recent(with in a day) historical data is stored in SQLite
- Uses Polars for efficient data manipulation and analysis

### Each market structure
Each market directory contains the files below;
- `market.rs` main file to provide `{ExchangeName} Market` object
- `message.rs` defines structs for REST and websocket data structure(JSON)
- `rest.rs` implements rest request for exchange according to each exchanges documentation.
- `ws.rs` implements realtime stream(public and private). Usually, its implement with WebSocket.


### Testing Structure
- Test files are organized in `test/` directory
- Exchange-specific tests in subdirectories (`binance_test/`, `bybit_test/`, etc.)
- Integration tests in `test/00_suite/`
- Manual testing notebooks in `test/manual/`


