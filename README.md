# Bybit Historical Data Downloader

A Python tool for downloading historical **1-hour OHLCV candles** for Bybit USDT linear perpetual contracts. It downloads multiple symbols concurrently and saves the results as individual CSV files and one combined CSV file.

> This project downloads public market data only. Bybit API keys are not required. It does not place orders or access private account information.

## Features

- Downloads currently trading USDT perpetual contracts from Bybit's `linear` market.
- Downloads 1-hour candles (`interval=60`) starting from **2020-01-01 UTC**.
- Stops at the most recently completed 1-hour candle.
- Uses `ThreadPoolExecutor` to download different symbols concurrently.
- Uses one shared request-rate limiter across the worker threads in the process.
- Retries failed requests with exponential backoff.
- Saves a separate CSV file for each symbol.
- Builds a combined CSV containing all downloaded symbols.
- Skips symbols that already have a valid per-symbol CSV file; use `--force` to download them again.
- Stores timestamps in UTC.

## Requirements

- **Python 3.10 or newer**. The pinned `pybit` release requires Python 3.10+. Python itself is not installed through `requirements.txt`; install it separately.
- Windows 10/11 is the documented setup in this guide. The Python script itself may also run on other operating systems with a compatible Python installation.
- An internet connection to access the Bybit public API and Python Package Index (PyPI).
- Git is needed only if you clone the project from GitHub.

Check that Python is installed:

```powershell
py --version
```

The output should show Python 3.10 or newer, for example `Python 3.13.x`.

If the `py` command is not found, install Python from [python.org](https://www.python.org/downloads/windows/) and enable the **Python Launcher** option during installation. If you have several Python versions installed, you can list them with:

```powershell
py -0p
```

## Installation

### Option A: Clone the repository from GitHub

Replace `<YOUR-GITHUB-USERNAME>` with the account that hosts this repository:

```powershell
git clone https://github.com/<YOUR-GITHUB-USERNAME>/bybit-historical-downloader.git
cd bybit-historical-downloader
```

### Option B: Download the ZIP archive

1. Open the repository page on GitHub.
2. Select **Code → Download ZIP**.
3. Extract the archive to a folder.
4. Open PowerShell in that folder.

### Create an isolated Python environment

It is recommended to install dependencies in a virtual environment so they do not affect other Python projects:

```powershell
py -m venv .venv
```

Install the dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The `requirements.txt` file pins the version of `pybit` used by this project. Pip automatically installs the packages required by `pybit` as well (including `requests`, `websocket-client`, and `pycryptodome`). You do not need to install those packages separately.

### Alternative: use the Windows installer script

On Windows, you can double-click `install_dependencies.bat`. It checks for the Python Launcher, creates the `.venv` environment if needed, and installs the packages listed in `requirements.txt` into that environment.

## Usage

Run the downloader from the project directory.

### Default settings

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py
```

The default settings are:

- `--workers 8`: up to 8 symbol-download tasks can run concurrently.
- `--rps 8`: the downloader starts no more than 8 API requests per second across all its worker threads.

### Set the number of workers and request rate manually

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py --workers 8 --rps 8
```

Parameters:

| Parameter | Description | Allowed values / default |
|---|---|---|
| `--workers` | Maximum number of symbols downloaded concurrently. | Integer from 1 to 32; default: `8` |
| `--rps` | Maximum request-start rate shared by all workers in this process. | Greater than 0 and up to 20; default: `8` |
| `--force` | Download all currently listed symbols again, replacing their per-symbol CSV files. | Flag; disabled by default |

For a cautious first run, use a lower rate such as:

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py --workers 5 --rps 5
```

Increasing `workers` does not necessarily make the download proportionally faster. The `--rps` limiter applies to this program process only; it does not coordinate separate copies of the downloader running at the same time. Bybit may also apply its own API limits, so do not assume that any particular request rate is guaranteed to be accepted.

### Download everything again

By default, symbols with an existing per-symbol CSV file containing the expected header are skipped. To download all symbols again:

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py --force --workers 8 --rps 8
```

### Show command-line help

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py --help
```

## Output files

The downloader creates the `bybit_data` directory next to the script:

```text
bybit-historical-downloader/
├── bybit_historical_downloader_fast_windows.py
├── requirements.txt
├── install_dependencies.bat
├── README.md
├── .gitignore
└── bybit_data/
    ├── by_symbol/
    │   ├── BTCUSDT.csv
    │   ├── ETHUSDT.csv
    │   └── ...
    ├── all_usdt_perpetual_1h.csv
    ├── failed_symbols.csv          # created if a symbol fails
    ├── error_tracebacks.txt        # created if an exception traceback is recorded
    └── run_report.txt
```

### CSV columns

Each candle row contains these columns:

| Column | Meaning |
|---|---|
| `timestamp` | Candle opening time in UTC, in ISO 8601 format with `+00:00` suffix |
| `open` | Opening price |
| `high` | Highest price during the candle |
| `low` | Lowest price during the candle |
| `close` | Closing price |
| `volume` | Traded volume returned by Bybit |
| `turnover` | Turnover returned by Bybit |
| `symbol` | Bybit trading symbol, for example `BTCUSDT` |

All timestamps are stored in UTC. The computer's local time zone and daylight-saving-time changes do not alter the timestamps saved in the CSV files.

The combined CSV is assembled by reading the individual symbol files in filename order. It is grouped by symbol; it is **not guaranteed to be globally sorted by timestamp across all symbols**.

### Log and report files

- `run_report.txt` — summary of the run, including UTC start/end times, counts, selected workers/RPS, and elapsed time.
- `failed_symbols.csv` — symbol, error message, and UTC time for each symbol that failed. This file is created only when a symbol fails; entries may accumulate across runs.
- `error_tracebacks.txt` — detailed Python traceback information for failed symbol tasks. It is created only when needed.

## Resuming after an interruption or failed symbols

Successful per-symbol CSV files are saved separately. On the next normal run, files with the expected CSV header are skipped, while symbols without a valid completed file are attempted again. This lets you rerun the downloader without downloading every successful symbol again.

If a request keeps failing, the downloader retries it up to 7 times with increasing delays. After the retries are exhausted, that symbol is recorded in `failed_symbols.csv` and the program continues with other symbols.

**Important:** if Bybit returns an IP-level `HTTP 403 access too frequent` response, stop the downloader manually and wait at least 10 minutes before trying again. The current script does not cancel every other worker automatically when one worker detects this response. Avoid running multiple copies of the downloader at the same time.

Use `--force` only when you intentionally want to download all symbols again.

## Troubleshooting

### `ModuleNotFoundError: No module named 'pybit'`

Install the dependencies into the same Python environment used to run the script:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Then run the script with that same interpreter:

```powershell
.\.venv\Scripts\python.exe .\bybit_historical_downloader_fast_windows.py --workers 8 --rps 8
```

### `py` is not recognized

Install Python from [python.org](https://www.python.org/downloads/windows/) and enable the Python Launcher. Reopen PowerShell after installation.

### SSL or connection errors while installing dependencies

Check your internet connection, VPN/proxy settings, and antivirus HTTPS inspection. Retry the install command. Do not disable HTTPS certificate verification as a workaround.

### Bybit API rate-limit errors

Reduce `--rps` and, if needed, `--workers`. If Bybit returns `HTTP 403 access too frequent`, stop the program and wait at least 10 minutes before restarting.

## Project status and limitations

- This project uses Bybit's public market-data API and does not require API credentials.
- The downloader is designed for 1-hour candles of USDT linear perpetual contracts; the interval and start date are constants in the Python source code.
- Download speed depends on network latency, Bybit API response times, the number of symbols, and API rate limits.
- GPU acceleration is not used because the main workload is waiting for network responses rather than performing GPU-suitable numerical calculations.

## License

No license has been specified in this repository yet. Add a `LICENSE` file before distributing the project under a particular open-source license.
