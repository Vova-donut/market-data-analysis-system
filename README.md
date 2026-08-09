# Market Data Analysis System

A real-time system for collecting, comparing, and analyzing live market data from multiple cryptocurrency exchanges.

The project was built to study price differences between exchanges and record how those differences change over time.

Instead of manually checking prices, the system connects to several exchanges at the same time, processes live market updates, detects spread events, and stores the results for later analysis.

> ⚠️ This repository is a portfolio version of a system originally built for real-world market research.

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![asyncio](https://img.shields.io/badge/asyncio-2C2D72?style=for-the-badge)
![WebSockets](https://img.shields.io/badge/WebSockets-010101?style=for-the-badge)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

---

## What Does It Do?

In simple terms:

1. Receives live prices from multiple exchanges.
2. Compares the same market across exchanges.
3. Detects when a meaningful price difference appears.
4. Tracks how long the difference exists.
5. Stores the event and related market data.
6. Makes the collected data available for further analysis.

---

## Architecture

Live exchange data is collected, normalized, compared in real time, and stored when meaningful spread events are detected.

![Market Data Analysis System Architecture](docs/architecture.png)

---

## Technical Highlights

- Real-time WebSocket data collection
- Multiple exchanges monitored simultaneously
- Asynchronous processing with Python
- Spread-event detection and tracking
- Persistent storage using SQLite
- Market event logging and analysis
- Designed for deployment on remote VPS servers

---

## Example Output

The system records detected price-difference events so they can be analyzed later.

A simplified example looks like this:

| Market | Buy Exchange | Sell Exchange | Start Spread | Max Spread | Duration |
|---|---|---|---:|---:|---:|
| BEATUSDT | Binance | OKX | 0.467% | 0.467% | 38 ms |
| JELLYJELLYUSDT | Binance | OKX | 0.314% | 0.703% | 26 ms |
| SAPIENUSDT | OKX | Binance | 0.315% | 0.315% | 20 ms |
| ZBTUSDT | OKX | Binance | 0.314% | 0.461% | 30 ms |
| SPKUSDT | Binance | OKX | 0.348% | 0.418% | 21 ms |

Duration shows how long the detected price difference remained active.

Two sample datasets are included:

- [`Recruiter-friendly sample`](sample_data/spread_events_recruiter_sample.csv) — simplified fields for quick understanding.
- [`Full technical sample`](sample_data/spread_events_full_sample.csv) — all recorded fields for deeper technical inspection.

More information about the datasets is available in [`sample_data/README.md`](sample_data/README.md).

---

## Deployment

The system was deployed and tested on VPS servers in multiple Asian regions:

- 🇯🇵 Tokyo
- 🇸🇬 Singapore
- 🇭🇰 Hong Kong

Multiple locations were used to compare network latency and market-data delivery behaviour between regions.

This helped evaluate how server location affects real-time data collection and event detection.

---

## Getting Started

### Requirements

- Python 3.10+
- pip

### Installation

Clone the repository:

```bash
git clone https://github.com/Vova-donut/market-data-analysis-system.git
cd market-data-analysis-system
```

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it:

**macOS / Linux**

```bash
source .venv/bin/activate
```

**Windows**

```bash
.venv\Scripts\activate
```

Install the required dependencies:

```bash
python -m pip install -r requirements.txt
```

Create a local environment configuration from the provided example:

**macOS / Linux**

```bash
cp .env.example .env
```

On Windows, copy `.env.example` and rename the copy to `.env`.

The default configuration uses Binance and the standard public OKX WebSocket feed and does not require API credentials.

Run the collector:

```bash
python -m src.main
```

### Optional OKX SBE Feed

The project also supports the OKX SBE binary market-data feed.

To enable it, update your `.env` file:

```env
OKX_USE_SBE=1
OKX_API_KEY_PARSER=your_api_key
OKX_SECRET_PARSER=your_secret_key
OKX_PASSWORD_PARSER=your_passphrase
```

OKX API credentials are not included in this repository. Users must provide their own credentials.

To use the standard public OKX WebSocket feed without API credentials, keep:

```env
OKX_USE_SBE=0
```

### Configuration

Additional settings such as active exchanges, spread thresholds, baseline window, database path, and WebSocket behaviour can be configured through the `.env` file.

See `.env.example` for the available configuration options.
