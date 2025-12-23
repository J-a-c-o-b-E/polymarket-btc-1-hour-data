# main.py
import csv
import json
import os
import time
import threading
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict

import httpx
import websockets

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError


# -----------------------------
# config (edit these per asset)
# -----------------------------

ASSET_CODE = "btc"  # "btc" | "eth" | "sol" | "xrp"
SEARCH_QUERY = "bitcoin up or down"  # btc: "bitcoin up or down" | eth: "ethereum up or down" | sol: "solana up or down" | xrp: "xrp up or down"
CHAINLINK_SYMBOL = "btc/usd"  # "btc/usd" | "eth/usd" | "sol/usd" | "xrp/usd"

QUOTE_NOTIONALS_USDC = [1, 10, 100, 1000, 10000]
POLL_SECONDS = 1.0

SESSION_SECONDS = 60 * 60  # 1 hour
WAIT_FOR_NEXT_FULL_SESSION = True  # if you start mid-hour, wait for the next market to start so every file is a full session

HTTP_TIMEOUT_SECONDS = 3.0
LOCAL_OUT_DIR = "session_csv"

# google drive target folder id (set this in Railway variables)
# this should be the folder for THIS script, eg Polymarket Data/btc_1h
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()

# oauth json blobs (set these in Railway variables)
GOOGLE_OAUTH_CLIENT_JSON = os.getenv("GOOGLE_OAUTH_CLIENT_JSON", "").strip()
GOOGLE_OAUTH_TOKEN_JSON = os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "").strip()

# -----------------------------
# endpoints
# -----------------------------

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"


# -----------------------------
# helpers
# -----------------------------

@dataclass
class MarketPick:
    slug: str
    start: datetime
    end: datetime
    up_token_id: str
    down_token_id: str


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(s: str) -> datetime:
    # gamma returns Z timestamps often
    s = (s or "").strip()
    if not s:
        raise ValueError("empty datetime")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def maybe_json(v):
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        t = v.strip()
        if not t:
            return v
        if (t.startswith("[") and t.endswith("]")) or (t.startswith("{") and t.endswith("}")):
            try:
                return json.loads(t)
            except Exception:
                return v
    return v


def resolve_up_down_tokens(market: dict) -> Tuple[str, str]:
    outcomes = maybe_json(market.get("outcomes"))
    token_ids = maybe_json(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) < 2 or len(token_ids) < 2:
        raise RuntimeError("gamma market missing outcomes or clobTokenIds")

    outcomes_norm = [str(x).strip().lower() for x in outcomes]
    token_norm = [str(x).strip() for x in token_ids]

    if "up" in outcomes_norm and "down" in outcomes_norm:
        up_id = token_norm[outcomes_norm.index("up")]
        down_id = token_norm[outcomes_norm.index("down")]
        return up_id, down_id

    # fallback
    return token_norm[0], token_norm[1]


def cents(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x * 100.0, 6)


def avg_fill_prices_for_notionals(asks: List[Tuple[float, float]], notionals: List[float]) -> List[Optional[float]]:
    results: List[Optional[float]] = [None] * len(notionals)

    remaining = [float(n) for n in notionals]
    spent = [0.0 for _ in notionals]
    shares = [0.0 for _ in notionals]
    done = [False for _ in notionals]
    unfinished = len(notionals)

    for price, size in asks:
        if unfinished == 0:
            break

        level_cash = price * size

        for i in range(len(notionals)):
            if done[i]:
                continue

            if remaining[i] <= 1e-12:
                done[i] = True
                unfinished -= 1
                continue

            if level_cash <= remaining[i] + 1e-12:
                spent[i] += level_cash
                shares[i] += size
                remaining[i] -= level_cash
                if remaining[i] <= 1e-12:
                    done[i] = True
                    unfinished -= 1
            else:
                take_shares = remaining[i] / price
                spent[i] += remaining[i]
                shares[i] += take_shares
                remaining[i] = 0.0
                done[i] = True
                unfinished -= 1

    for i in range(len(notionals)):
        results[i] = (spent[i] / shares[i]) if shares[i] > 0 else None

    return results


# -----------------------------
# realtime chainlink price feed via RTDS
# -----------------------------

class ChainlinkPriceFeed:
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self._lock = threading.Lock()
        self.latest_price: Optional[float] = None
        self.latest_ts_ms: Optional[int] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def get_latest(self) -> Tuple[Optional[float], Optional[int]]:
        with self._lock:
            return self.latest_price, self.latest_ts_ms

    def _run_forever(self):
        asyncio.run(self._ws_loop())

    async def _ws_loop(self):
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": self.symbol})}
            ],
        }

        backoff = 0.5
        while not self._stop.is_set():
            try:
                async with websockets.connect(RTDS_WS_URL, ping_interval=None) as ws:
                    await ws.send(json.dumps(sub_msg))
                    last_ping = time.time()

                    while not self._stop.is_set():
                        if time.time() - last_ping >= 5.0:
                            try:
                                await ws.ping()
                            except Exception:
                                break
                            last_ping = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue

                        payload = msg.get("payload") or {}
                        if (payload.get("symbol") or "").lower() != self.symbol:
                            continue

                        value = payload.get("value")
                        ts_ms = payload.get("timestamp") or msg.get("timestamp")

                        if isinstance(value, (int, float)) and isinstance(ts_ms, int):
                            with self._lock:
                                self.latest_price = float(value)
                                self.latest_ts_ms = int(ts_ms)

                backoff = 0.5
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.6, 10.0)


# -----------------------------
# gamma discovery (hourly market selection)
# -----------------------------

def make_http_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": f"pm-{ASSET_CODE}-hourly-collector/1.0"},
        timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=0),
        http2=False,
    )


def gamma_public_search(http: httpx.Client, q: str) -> dict:
    r = http.get(
        f"{GAMMA_API}/public-search",
        params={
            "q": q,
            "limit_per_type": 50,
            "events_status": "active",
            "keep_closed_markets": 0,
        },
    )
    r.raise_for_status()
    return r.json()


def pick_hourly_market(http: httpx.Client, q: str, session_seconds: int, wait_for_next_full: bool) -> Optional[MarketPick]:
    now = datetime.now(timezone.utc)

    try:
        data = gamma_public_search(http, q)
    except Exception:
        return None

    candidates: List[Tuple[datetime, datetime, dict]] = []

    for ev in (data.get("events") or []):
        for m in (ev.get("markets") or []):
            try:
                if not m.get("active", False) or m.get("closed", False):
                    continue

                start_s = m.get("startDate")
                end_s = m.get("endDate")
                if not start_s or not end_s:
                    continue

                start = parse_dt(start_s)
                end = parse_dt(end_s)

                dur = (end - start).total_seconds()
                if abs(dur - session_seconds) > 180:  # allow some slack
                    continue

                candidates.append((start, end, m))
            except Exception:
                continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])  # by start

    # current session
    current = [c for c in candidates if c[0] <= now < c[1]]
    upcoming = [c for c in candidates if c[0] > now]

    chosen = None
    if current:
        if wait_for_next_full:
            chosen = upcoming[0] if upcoming else None
        else:
            chosen = current[-1]  # most recent started
    else:
        chosen = upcoming[0] if upcoming else None

    if not chosen:
        return None

    start, end, m = chosen
    up_id, down_id = resolve_up_down_tokens(m)

    return MarketPick(
        slug=str(m.get("slug")),
        start=start,
        end=end,
        up_token_id=up_id,
        down_token_id=down_id,
    )


# -----------------------------
# clob books (single request for up+down)
# -----------------------------

def fetch_books_safe(
    http: httpx.Client,
    token_ids: List[str],
    prev: Dict[str, List[Tuple[float, float]]],
    retries: int = 3,
) -> Dict[str, List[Tuple[float, float]]]:
    payload = [{"token_id": tid} for tid in token_ids]

    for attempt in range(retries):
        try:
            r = http.post(f"{CLOB_API}/books", json=payload)
            r.raise_for_status()
            arr = r.json()
            out = dict(prev)

            if isinstance(arr, list):
                for book in arr:
                    try:
                        tid = str(book.get("asset_id") or "")
                        asks = book.get("asks") or []
                        levels: List[Tuple[float, float]] = []
                        for a in asks:
                            try:
                                p = float(a["price"])
                                s = float(a["size"])
                                if p > 0 and s > 0:
                                    levels.append((p, s))
                            except Exception:
                                continue
                        if levels:
                            levels.sort(key=lambda x: x[0])
                            out[tid] = levels
                    except Exception:
                        continue

            return out
        except Exception:
            time.sleep(0.05 * (2 ** attempt))

    return prev


# -----------------------------
# google drive uploader (oauth token)
# -----------------------------

class DriveUploaderOAuth:
    def __init__(self, client_json_str: str, token_json_str: str):
        scopes = ["https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        token_info = json.loads(token_json_str)
        creds = Credentials.from_authorized_user_info(token_info, scopes=scopes)

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())

        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def upload_or_update(self, local_path: str, drive_folder_id: str, drive_filename: str):
        q = f"name='{drive_filename}' and '{drive_folder_id}' in parents and trashed=false"
        res = self.service.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
        files = res.get("files", [])

        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)

        if files:
            file_id = files[0]["id"]
            self.service.files().update(fileId=file_id, media_body=media).execute()
        else:
            metadata = {"name": drive_filename, "parents": [drive_folder_id]}
            self.service.files().create(body=metadata, media_body=media, fields="id").execute()


# -----------------------------
# csv helpers
# -----------------------------

def write_header(writer: csv.writer):
    headers = ["timestamp_utc"]
    for n in QUOTE_NOTIONALS_USDC:
        headers.append(f"up_buy_{n}_avg_cents")
        headers.append(f"down_buy_{n}_avg_cents")
    headers.append(f"{ASSET_CODE}_chainlink_usd")
    writer.writerow(headers)


def main():
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("missing DRIVE_FOLDER_ID env var")
    if not GOOGLE_OAUTH_CLIENT_JSON or not GOOGLE_OAUTH_TOKEN_JSON:
        raise RuntimeError("missing GOOGLE_OAUTH_CLIENT_JSON or GOOGLE_OAUTH_TOKEN_JSON env var")

    os.makedirs(LOCAL_OUT_DIR, exist_ok=True)

    drive = DriveUploaderOAuth(GOOGLE_OAUTH_CLIENT_JSON, GOOGLE_OAUTH_TOKEN_JSON)

    # one-time startup test upload
    test_path = os.path.join(LOCAL_OUT_DIR, f"drive_test_{ASSET_CODE}_1h.csv")
    with open(test_path, "w", encoding="utf-8") as tf:
        tf.write("ok,utc\n")
        tf.write(f"1,{utc_iso_now()}\n")
    try:
        drive.upload_or_update(test_path, DRIVE_FOLDER_ID, os.path.basename(test_path))
        print("drive_test_upload_ok")
    except Exception as e:
        print("drive_test_upload_failed", type(e).__name__, repr(e))

    feed = ChainlinkPriceFeed(CHAINLINK_SYMBOL)
    feed.start()

    http = make_http_client()

    notionals = [float(x) for x in QUOTE_NOTIONALS_USDC]
    cached_asks: Dict[str, List[Tuple[float, float]]] = {}

    active: Optional[MarketPick] = None
    local_file = None
    f = None
    w = None
    rows_written = 0

    def close_and_upload(path: str, filename: str) -> bool:
        for attempt in range(5):
            try:
                drive.upload_or_update(path, DRIVE_FOLDER_ID, filename)
                return True
            except HttpError as e:
                print("upload_error_http", "attempt", attempt + 1, "status", getattr(e, "status_code", None), "error", str(e))
            except Exception as e:
                print("upload_error", "attempt", attempt + 1, "type", type(e).__name__, "error", repr(e))
            time.sleep(0.5 * (2 ** attempt))
        return False

    try:
        while True:
            # select the next hourly market (active or upcoming)
            if active is None:
                active = pick_hourly_market(http, SEARCH_QUERY, SESSION_SECONDS, WAIT_FOR_NEXT_FULL_SESSION)
                if active is None:
                    print("market_pick_failed", SEARCH_QUERY)
                    time.sleep(2.0)
                    continue

                # wait until its start for perfect full-session files
                now = datetime.now(timezone.utc)
                if now < active.start:
                    wait_s = (active.start - now).total_seconds()
                    print("next_session", active.slug, "starts_in_seconds", int(wait_s))
                    # sleep in chunks so we can refresh if needed
                    while True:
                        now2 = datetime.now(timezone.utc)
                        if now2 >= active.start:
                            break
                        time.sleep(min(5.0, (active.start - now2).total_seconds()))

                # open a new file per session
                rows_written = 0
                cached_asks = {}
                local_file = os.path.join(LOCAL_OUT_DIR, f"{active.slug}.csv")
                f = open(local_file, "w", newline="", encoding="utf-8")
                w = csv.writer(f)
                write_header(w)
                print("session_started", active.slug, "start", active.start.isoformat(), "end", active.end.isoformat(), "file", local_file)

            # stop exactly when session ends
            now = datetime.now(timezone.utc)
            if now >= active.end:
                try:
                    f.flush()
                    f.close()
                except Exception:
                    pass

                fname = os.path.basename(local_file)
                ok = close_and_upload(local_file, fname)
                print("session_uploaded" if ok else "session_upload_failed", fname, "rows", rows_written)

                active = None
                continue

            loop_start = time.perf_counter()

            # fetch both books in one request
            try:
                cached_asks = fetch_books_safe(
                    http,
                    [active.up_token_id, active.down_token_id],
                    cached_asks,
                    retries=2,
                )
            except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError, httpx.PoolTimeout):
                try:
                    http.close()
                except Exception:
                    pass
                http = make_http_client()
                time.sleep(0.1)
                continue

            up_asks = cached_asks.get(active.up_token_id) or []
            down_asks = cached_asks.get(active.down_token_id) or []
            if not up_asks or not down_asks:
                time.sleep(0.05)
                continue

            up_avg = avg_fill_prices_for_notionals(up_asks, notionals)
            down_avg = avg_fill_prices_for_notionals(down_asks, notionals)

            px, _ = feed.get_latest()
            ts = utc_iso_now()

            row = [ts]
            for i in range(len(notionals)):
                row.extend([cents(up_avg[i]), cents(down_avg[i])])
            row.append(px)

            w.writerow(row)
            f.flush()
            rows_written += 1

            # lightweight heartbeat
            if rows_written % 10 == 0:
                print("tick", ts, "slug", active.slug, "up_1c", cents(up_avg[0]), "down_1c", cents(down_avg[0]), "px", px, "rows", rows_written)

            elapsed = time.perf_counter() - loop_start
            sleep_for = POLL_SECONDS - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("stopping_keyboard_interrupt")
    finally:
        try:
            if f:
                f.flush()
                f.close()
        except Exception:
            pass
        try:
            http.close()
        except Exception:
            pass
        feed.stop()

        # if we were in a session, try to upload the partial file once
        try:
            if local_file and os.path.exists(local_file):
                fname = os.path.basename(local_file)
                ok = close_and_upload(local_file, fname)
                print("final_upload_ok" if ok else "final_upload_failed", fname)
        except Exception:
            pass


if __name__ == "__main__":
    main()
