# Market Data Analysis System

A real-time system for collecting, comparing, and analyzing live market data from multiple cryptocurrency exchanges.

The project was built to study price differences between exchanges and record how those differences change over time.

Instead of manually checking prices, the system connects to several exchanges at the same time, processes live market updates, detects spread events, and stores the results for later analysis.

> ⚠️ This repository is a portfolio version of a system originally built for real-world market research.

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

Architecture diagram coming soon.

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

## Deployment

The system was tested on VPS servers in:

- Tokyo
- Singapore
- Hong Kong

Different server locations were used to evaluate network latency and data delivery behaviour.

---

## Tech Stack

- Python
- asyncio
- WebSockets
- SQLite
- Git

---

## Example Output

Sample output coming soon.