# -*- coding: utf-8 -*-
"""Fast Bybit USDT perpetual 1H historical downloader.

Main improvements over the serial version:
- Downloads different symbols concurrently with ThreadPoolExecutor.
- Uses one HTTP session per worker thread.
- Applies one shared request-rate limiter to all workers.
- Writes per-symbol files atomically, so an interrupted write is not
  mistaken for a completed download on the next run.
- Rebuilds the combined CSV only when data changed (or --force is used).
- Keeps the original CSV columns and output folder structure.

Public market data only. API keys are not required.

Examples:
    python bybit_historical_downloader_fast.py
    python bybit_historical_downloader_fast.py --workers 8 --rps 8
    python bybit_historical_downloader_fast.py --force --workers 6 --rps 6
"""

import argparse
import csv
import random
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from pybit.unified_trading import HTTP


# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------

CATEGORY = "linear"
INTERVAL = "60"           # 1 hour
LIMIT = 1000               # Bybit maximum for this endpoint
START_DATE = "2020-01-01"
HOUR_MS = 60 * 60 * 1000

MAX_RETRIES = 7
BASE_DELAY = 1.5
DEFAULT_WORKERS = 8
DEFAULT_REQUESTS_PER_SECOND = 8.0

OUTPUT_DIR = Path("bybit_data")
SYMBOL_DIR = OUTPUT_DIR / "by_symbol"

COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "symbol",
]

MIN_TIMESTAMP_MS = 946684800000       # 2000-01-01
MAX_TIMESTAMP_MS = 4102444800000      # 2100-01-01


# Each worker thread owns its own HTTP session. Do not share a pybit
# requests.Session object between worker threads.
_thread_local = threading.local()
_print_lock = threading.Lock()


# ---------------------------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------------------------

class RequestRateLimiter:
    """A process-wide limiter that spaces request starts across all workers."""

    def __init__(self, requests_per_second: float):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero")
        self.interval = 1.0 / requests_per_second
        self.next_request_at = 0.0
        self.lock = threading.Lock()

    def wait(self):
        # Hold the lock while waiting so simultaneous workers cannot all
        # pass the limiter at once.
        with self.lock:
            now = time.monotonic()
            delay = self.next_request_at - now
            if delay > 0:
                time.sleep(delay)
            self.next_request_at = max(self.next_request_at, time.monotonic()) + self.interval


def safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


# ---------------------------------------------------------------------
# TIME HELPERS
# ---------------------------------------------------------------------

def date_to_ms(date_string: str) -> int:
    dt = datetime.strptime(date_string, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def last_closed_hour_ms() -> int:
    now_ms = int(time.time() * 1000)
    current_hour_start = (now_ms // HOUR_MS) * HOUR_MS
    return current_hour_start - 1


def timestamp_to_utc(timestamp_ms: int) -> str:
    seconds = timestamp_ms // 1000
    milliseconds = timestamp_ms % 1000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{milliseconds:03d}+00:00"


def safe_timestamp(value) -> int:
    try:
        result = int(str(value).strip())
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"Invalid timestamp: {value!r}") from exc

    if result < MIN_TIMESTAMP_MS or result > MAX_TIMESTAMP_MS:
        raise ValueError(f"Timestamp outside allowed range: {result}")
    return result


# ---------------------------------------------------------------------
# FILE HELPERS
# ---------------------------------------------------------------------

def prepare_directories():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SYMBOL_DIR.mkdir(parents=True, exist_ok=True)


def symbol_csv_path(symbol: str) -> Path:
    return SYMBOL_DIR / f"{symbol}.csv"


def is_symbol_completed(symbol: str) -> bool:
    """A valid final CSV exists. New files are committed with atomic replace."""
    path = symbol_csv_path(symbol)
    if not path.exists():
        return False

    try:
        with path.open("r", newline="", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.reader(f)
            return next(reader, None) == COLUMNS
    except Exception:
        return False


def save_symbol_csv(symbol: str, rows):
    """Write to a temporary file first, then atomically replace the final CSV."""
    final_path = symbol_csv_path(symbol)
    temp_path = final_path.with_name(final_path.name + ".tmp")

    with temp_path.open(
        "w", newline="", encoding="utf-8-sig", errors="replace"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
        f.flush()

    temp_path.replace(final_path)


def safe_log_error(symbol: str, error):
    log_file = OUTPUT_DIR / "failed_symbols.csv"
    try:
        file_is_empty = not log_file.exists() or log_file.stat().st_size == 0
        with log_file.open("a", newline="", encoding="utf-8", errors="replace") as f:
            writer = csv.writer(f)
            if file_is_empty:
                writer.writerow(["symbol", "error", "time_utc"])
            writer.writerow([
                symbol,
                str(error),
                datetime.now(timezone.utc).isoformat(),
            ])
    except Exception as log_error:
        safe_print(f"[WARNING] Could not write error log: {log_error}", file=sys.stderr)


def append_traceback(symbol: str):
    try:
        traceback_file = OUTPUT_DIR / "error_tracebacks.txt"
        with traceback_file.open("a", encoding="utf-8", errors="replace") as f:
            f.write("\n" + "=" * 70 + "\n")
            f.write(f"Symbol: {symbol}\n")
            f.write(traceback.format_exc())
    except Exception:
        pass


# ---------------------------------------------------------------------
# API HELPERS
# ---------------------------------------------------------------------

def get_thread_session() -> HTTP:
    """Create/reuse one pybit HTTP session in the current worker thread."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = HTTP(
            testnet=False,
            timeout=30,
            max_retries=1,
        )
        _thread_local.session = session
    return session


def api_call_with_retry(function, description: str, limiter: RequestRateLimiter):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        limiter.wait()
        try:
            result = function()

            if not isinstance(result, dict):
                raise RuntimeError(f"Unexpected API response for {description}")

            ret_code = result.get("retCode", 0)
            if ret_code != 0:
                raise RuntimeError(
                    f"Bybit API error {ret_code}: {result.get('retMsg', 'Unknown error')}"
                )
            return result

        except Exception as error:
            last_error = error
            error_text = str(error).lower()

            # Bybit says an IP-level 403/access-too-frequent block requires
            # stopping requests and waiting for the block to expire.
            if "403" in error_text and "access too frequent" in error_text:
                raise RuntimeError(
                    f"{description}: Bybit returned HTTP 403 (access too frequent). "
                    "Stop the downloader and wait at least 10 minutes before retrying."
                ) from error

            if attempt >= MAX_RETRIES:
                break

            delay = min(BASE_DELAY * (2 ** (attempt - 1)), 60.0)
            delay += random.uniform(0.0, 0.75)
            safe_print(
                f"\n[RETRY] {description}: {error}\n"
                f"Attempt {attempt}/{MAX_RETRIES}; waiting {delay:.1f} sec..."
            )
            time.sleep(delay)

    raise RuntimeError(
        f"{description} failed after {MAX_RETRIES} attempts: {last_error}"
    ) from last_error


def get_all_instruments(session: HTTP, limiter: RequestRateLimiter):
    safe_print("Получение списка инструментов...")

    instruments = []
    cursor = None
    seen_cursors = set()

    while True:
        params = {"category": CATEGORY, "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        response = api_call_with_retry(
            lambda p=params: session.get_instruments_info(**p),
            "Get instruments info",
            limiter,
        )

        result = response.get("result", {})
        instruments.extend(result.get("list", []))
        next_cursor = result.get("nextPageCursor")

        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise RuntimeError("Repeated pagination cursor received from Bybit")

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    safe_print(f"Всего инструментов получено (linear): {len(instruments)}")

    filtered = []
    for item in instruments:
        if item.get("contractType") != "LinearPerpetual":
            continue
        if item.get("quoteCoin") != "USDT":
            continue
        if item.get("settleCoin") != "USDT":
            continue
        if item.get("status") != "Trading":
            continue
        if item.get("symbol"):
            filtered.append(item)

    filtered.sort(key=lambda item: item["symbol"])
    safe_print(f"Найдено USDT Perpetual контрактов: {len(filtered)}")
    return filtered


# ---------------------------------------------------------------------
# KLINE DOWNLOAD
# ---------------------------------------------------------------------

def download_symbol_history(instrument, global_start_ms: int, global_end_ms: int,
                            limiter: RequestRateLimiter):
    """Download one symbol. Different symbols run in parallel."""
    symbol = instrument["symbol"]
    session = get_thread_session()

    launch_time = instrument.get("launchTime")
    try:
        launch_ms = int(str(launch_time))
    except (TypeError, ValueError, OverflowError):
        launch_ms = global_start_ms

    start_ms = max(global_start_ms, launch_ms) if launch_ms > 0 else global_start_ms
    if start_ms > global_end_ms:
        return []

    cursor_end = int(global_end_ms)
    candles = {}  # timestamp -> CSV row; also protects against duplicate candles

    while cursor_end >= start_ms:
        request_end = int(cursor_end)

        response = api_call_with_retry(
            lambda e=request_end: session.get_kline(
                category=CATEGORY,
                symbol=symbol,
                interval=INTERVAL,
                limit=LIMIT,
                end=e,
            ),
            f"Get kline {symbol}",
            limiter,
        )

        raw_list = response.get("result", {}).get("list", [])
        if not raw_list:
            break

        page_timestamps = []
        for candle in raw_list:
            if not isinstance(candle, (list, tuple)) or len(candle) < 7:
                continue

            timestamp_ms = safe_timestamp(candle[0])
            if timestamp_ms < start_ms or timestamp_ms > global_end_ms:
                continue

            page_timestamps.append(timestamp_ms)
            candles[timestamp_ms] = [
                timestamp_to_utc(timestamp_ms),
                str(candle[1]),
                str(candle[2]),
                str(candle[3]),
                str(candle[4]),
                str(candle[5]),
                str(candle[6]),
                symbol,
            ]

        if not page_timestamps:
            break

        oldest_timestamp = min(page_timestamps)
        if oldest_timestamp <= start_ms:
            break

        next_end = oldest_timestamp - 1
        if next_end >= cursor_end:
            raise RuntimeError(f"Pagination did not move backwards for {symbol}")

        cursor_end = int(next_end)

    return [candles[timestamp] for timestamp in sorted(candles)]


def download_and_save_symbol(instrument, start_ms: int, end_ms: int,
                             limiter: RequestRateLimiter):
    symbol = instrument["symbol"]
    rows = download_symbol_history(instrument, start_ms, end_ms, limiter)
    save_symbol_csv(symbol, rows)
    return symbol, len(rows)


# ---------------------------------------------------------------------
# COMBINED CSV
# ---------------------------------------------------------------------

def rebuild_combined_csv():
    """Stream per-symbol CSVs into one file without loading the dataset in RAM."""
    combined_path = OUTPUT_DIR / "all_usdt_perpetual_1h.csv"
    temp_path = combined_path.with_name(combined_path.name + ".tmp")
    symbol_files = sorted(SYMBOL_DIR.glob("*.csv"))

    safe_print("\nСоздание общего CSV...")
    total_rows = 0

    with temp_path.open("w", newline="", encoding="utf-8-sig", errors="replace") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(COLUMNS)

        for file_path in symbol_files:
            try:
                with file_path.open("r", newline="", encoding="utf-8-sig", errors="replace") as input_file:
                    reader = csv.reader(input_file)
                    header = next(reader, None)
                    if header != COLUMNS:
                        safe_print(f"[WARNING] Invalid CSV header: {file_path.name}")
                        continue

                    for row in reader:
                        if not row or not row[0]:
                            continue
                        if len(row) < len(COLUMNS):
                            row.extend([""] * (len(COLUMNS) - len(row)))
                        writer.writerow(row[:len(COLUMNS)])
                        total_rows += 1
            except Exception as error:
                safe_print(f"[WARNING] Cannot read {file_path.name}: {error}")

    temp_path.replace(combined_path)
    safe_print(f"Общий CSV создан. Строк данных: {total_rows:,}")
    return total_rows


# ---------------------------------------------------------------------
# REPORT
# ---------------------------------------------------------------------

def write_report(total_instruments, successful, failed, skipped, total_candles,
                 start_time, end_time, elapsed, workers, rps):
    report_path = OUTPUT_DIR / "run_report.txt"
    lines = [
        "BYBIT HISTORICAL DATA DOWNLOAD REPORT",
        "=" * 45,
        f"Run started UTC: {start_time}",
        f"Run finished UTC: {end_time}",
        f"Total instruments: {total_instruments}",
        f"Successful: {successful}",
        f"Failed: {failed}",
        f"Skipped: {skipped}",
        f"Candles downloaded this run: {total_candles:,}",
        f"Workers: {workers}",
        f"Request rate limit: {rps:.2f} requests/second",
        f"Elapsed seconds: {elapsed:.1f}",
        f"Elapsed minutes: {elapsed / 60:.1f}",
        "",
        "Files:",
        str(SYMBOL_DIR),
        str(OUTPUT_DIR / "all_usdt_perpetual_1h.csv"),
        str(OUTPUT_DIR / "failed_symbols.csv"),
    ]
    with report_path.open("w", encoding="utf-8-sig", errors="replace") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Concurrent downloader for Bybit USDT perpetual 1H candles"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Download all symbols again, overwriting their CSV files",
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Number of concurrent symbol downloads (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--rps", type=float, default=DEFAULT_REQUESTS_PER_SECOND,
        help=f"Maximum request starts per second across all workers (default: {DEFAULT_REQUESTS_PER_SECOND})",
    )
    args = parser.parse_args()

    if args.workers < 1 or args.workers > 32:
        parser.error("--workers must be between 1 and 32")
    if args.rps <= 0 or args.rps > 20:
        parser.error("--rps must be greater than 0 and no more than 20")

    prepare_directories()
    limiter = RequestRateLimiter(args.rps)

    start_ms = date_to_ms(START_DATE)
    end_ms = last_closed_hour_ms()
    start_run = time.time()
    start_run_utc = datetime.now(timezone.utc).isoformat()

    safe_print("=" * 64)
    safe_print("BYBIT USDT PERPETUAL HISTORICAL DOWNLOADER — FAST MODE")
    safe_print("=" * 64)
    safe_print(f"Start date: {START_DATE} UTC")
    safe_print(f"Last fully closed hour: {timestamp_to_utc(end_ms - (end_ms % HOUR_MS))}")
    safe_print(f"Output folder: {OUTPUT_DIR.resolve()}")
    safe_print(f"Workers: {args.workers}")
    safe_print(f"Request limit: {args.rps:.2f} requests/second")
    safe_print(f"Force mode: {args.force}")
    safe_print("=" * 64)

    # This session is used only by the main thread for instrument discovery.
    main_session = HTTP(testnet=False, timeout=30, max_retries=1)
    instruments = get_all_instruments(main_session, limiter)

    pending = []
    skipped = 0
    for instrument in instruments:
        symbol = instrument["symbol"]
        if not args.force and is_symbol_completed(symbol):
            skipped += 1
        else:
            pending.append(instrument)

    safe_print(f"Уже загружено и пропущено: {skipped}")
    safe_print(f"Осталось загрузить: {len(pending)}")

    successful = 0
    failed = 0
    total_candles = 0
    data_changed = bool(args.force)

    if pending:
        safe_print(f"\nЗапуск {args.workers} параллельных загрузчиков...\n")
        completed = 0

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_symbol = {
                executor.submit(
                    download_and_save_symbol,
                    instrument,
                    start_ms,
                    end_ms,
                    limiter,
                ): instrument["symbol"]
                for instrument in pending
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                elapsed = time.time() - start_run
                average = elapsed / completed
                remaining_minutes = average * (len(pending) - completed) / 60

                try:
                    returned_symbol, candle_count = future.result()
                    successful += 1
                    total_candles += candle_count
                    data_changed = True
                    safe_print(
                        f"[{completed}/{len(pending)}] [OK] {returned_symbol}: "
                        f"{candle_count:,} candles | ETA: {remaining_minutes:.1f} min"
                    )
                except Exception as error:
                    failed += 1
                    safe_print(f"[{completed}/{len(pending)}] [ERROR] {symbol}: {error}", file=sys.stderr)
                    append_traceback(symbol)
                    safe_log_error(symbol, error)

    # No need to rebuild a potentially large aggregate file on a run where
    # every symbol was skipped and no per-symbol data changed.
    combined_path = OUTPUT_DIR / "all_usdt_perpetual_1h.csv"
    if data_changed or not combined_path.exists():
        combined_rows = rebuild_combined_csv()
    else:
        combined_rows = 0
        safe_print("\nДанные не изменились — общий CSV оставлен без пересборки.")

    elapsed = time.time() - start_run
    finish_utc = datetime.now(timezone.utc).isoformat()

    write_report(
        total_instruments=len(instruments),
        successful=successful,
        failed=failed,
        skipped=skipped,
        total_candles=total_candles,
        start_time=start_run_utc,
        end_time=finish_utc,
        elapsed=elapsed,
        workers=args.workers,
        rps=args.rps,
    )

    safe_print("\n" + "=" * 64)
    safe_print("ЗАГРУЗКА ЗАВЕРШЕНА")
    safe_print("=" * 64)
    safe_print(f"Всего инструментов: {len(instruments)}")
    safe_print(f"Успешно обработано сейчас: {successful}")
    safe_print(f"Пропущено ранее загруженных: {skipped}")
    safe_print(f"Ошибок: {failed}")
    safe_print(f"Новых свечей за этот запуск: {total_candles:,}")
    if combined_rows:
        safe_print(f"Строк в общем CSV: {combined_rows:,}")
    safe_print(f"Время работы: {elapsed / 60:.1f} минут")
    safe_print(f"Результаты: {OUTPUT_DIR.resolve()}")
    safe_print("=" * 64)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        safe_print("\nОстановлено пользователем.")
        safe_print(
            "Уже сохранённые CSV останутся на месте. "
            "Запустите скрипт снова для продолжения."
        )
    except Exception as fatal_error:
        safe_print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {fatal_error}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
