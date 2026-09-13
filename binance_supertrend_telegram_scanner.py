 #!/usr/bin/env python3

import os
import asyncio
import math
import time
from datetime import datetime, timezone

import aiohttp


# =========================
# SETTINGS
# =========================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BINANCE_REST = "https://fapi.binance.com"

INTERVALS = ["30m", "1h"]

FACTOR = 2.0
ATR_LEN = 20
RSI_LEN = 14
DIVIDE_RSI_BY = 100.0

HISTORY_LIMIT = 120

# مهم: نخلي الطلبات بهدوء حتى Binance ما يعطيناش 429
REQUEST_DELAY = 0.15


# =========================
# INDICATOR FUNCTIONS
# =========================

def rma(values, length):
    out = [math.nan] * len(values)

    start = None
    for i in range(len(values) - length + 1):
        window = values[i:i + length]
        if all(not math.isnan(x) for x in window):
            start = i + length - 1
            seed = sum(window) / length
            out[start] = seed
            break

    if start is None:
        return out

    prev = out[start]
    alpha = 1.0 / length

    for i in range(start + 1, len(values)):
        x = values[i]
        if math.isnan(x):
            out[i] = prev
        else:
            prev = alpha * x + (1.0 - alpha) * prev
            out[i] = prev

    return out


def pine_rsi(closes, length):
    gains = [math.nan] * len(closes)
    losses = [math.nan] * len(closes)

    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains[i] = max(diff, 0.0)
        losses[i] = max(-diff, 0.0)

    avg_gain = rma(gains, length)
    avg_loss = rma(losses, length)

    result = [math.nan] * len(closes)

    for i in range(len(closes)):
        g = avg_gain[i]
        l = avg_loss[i]

        if math.isnan(g) or math.isnan(l):
            continue

        if l == 0:
            result[i] = 100.0
        elif g == 0:
            result[i] = 0.0
        else:
            rs = g / l
            result[i] = 100.0 - (100.0 / (1.0 + rs))

    return result


def calculate_direction(candles):
    highs = [x[2] for x in candles]
    lows = [x[3] for x in candles]
    closes = [x[4] for x in candles]

    n = len(candles)

    tr = [math.nan] * n

    for i in range(n):
        if i == 0:
            tr[i] = highs[i] - lows[i]
        else:
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )

    atr = rma(tr, ATR_LEN)
    rsi = pine_rsi(closes, RSI_LEN)

    lower2 = [math.nan] * n
    upper2 = [math.nan] * n
    superdir2 = [math.nan] * n
    direction2 = [1] * n

    for i in range(n):
        if math.isnan(atr[i]) or math.isnan(rsi[i]):
            continue

        src = (highs[i] + lows[i]) / 2.0
        a_rsi = abs(rsi[i] - 50.0) / DIVIDE_RSI_BY

        raw_lower = src - FACTOR * atr[i]
        raw_upper = src + FACTOR * atr[i]

        raw_lower = raw_lower - raw_lower * a_rsi
        raw_upper = raw_upper + raw_upper * a_rsi

        if i == 0 or math.isnan(lower2[i - 1]):
            prev_lower = 0.0
        else:
            prev_lower = lower2[i - 1]

        if i == 0 or math.isnan(upper2[i - 1]):
            prev_upper = 0.0
        else:
            prev_upper = upper2[i - 1]

        prev_close = closes[i - 1] if i > 0 else closes[i]

        if raw_lower > prev_lower or prev_close < prev_lower:
            lower2[i] = raw_lower
        else:
            lower2[i] = prev_lower

        if raw_upper < prev_upper or prev_close > prev_upper:
            upper2[i] = raw_upper
        else:
            upper2[i] = prev_upper

        atr_prev_is_na = i == 0 or math.isnan(atr[i - 1])

        if atr_prev_is_na:
            direction2[i] = 1
        else:
            prev_super = superdir2[i - 1]

            if (
                not math.isnan(prev_super)
                and not math.isnan(upper2[i - 1])
                and prev_super == upper2[i - 1]
            ):
                direction2[i] = -1 if closes[i] > upper2[i] else 1
            else:
                direction2[i] = 1 if closes[i] < lower2[i] else -1

        superdir2[i] = (
            lower2[i] if direction2[i] == -1 else upper2[i]
        )

    return int(direction2[-1])


# =========================
# BINANCE
# =========================

async def request_json(session, url, params=None, retries=8):
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=30) as r:

                if r.status == 429:
                    retry_after = r.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else 3 + attempt
                    print(f"429 Binance rate limit. Sleeping {wait}s", flush=True)
                    await asyncio.sleep(wait)
                    continue

                if r.status != 200:
                    body = await r.text()
                    print("HTTP ERROR", r.status, body[:200], flush=True)
                    await asyncio.sleep(2 + attempt)
                    continue

                return await r.json()

        except Exception as e:
            print("Request error:", e, flush=True)
            await asyncio.sleep(2 + attempt)

    return None


async def get_symbols(session):
    data = await request_json(
        session,
        f"{BINANCE_REST}/fapi/v1/exchangeInfo"
    )

    symbols = []

    if not data:
        return symbols

    for s in data.get("symbols", []):
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
        ):
            symbols.append(s["symbol"].lower())

    return sorted(symbols)


async def get_history(session, symbol, interval, limit=HISTORY_LIMIT):
    await asyncio.sleep(REQUEST_DELAY)

    data = await request_json(
        session,
        f"{BINANCE_REST}/fapi/v1/klines",
        {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        },
    )

    if not data:
        return []

    now_ms = int(time.time() * 1000)

    candles = []

    for k in data:
        close_time = int(k[6])

        # نستعمل فقط الشموع المقفلة
        if close_time >= now_ms:
            continue

        candles.append([
            int(k[0]),
            float(k[1]),
            float(k[2]),
            float(k[3]),
            float(k[4]),
        ])

    return candles


async def get_latest_closed(session, symbol, interval):
    await asyncio.sleep(REQUEST_DELAY)

    data = await request_json(
        session,
        f"{BINANCE_REST}/fapi/v1/klines",
        {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": 3,
        },
    )

    if not data:
        return None

    now_ms = int(time.time() * 1000)

    closed = [k for k in data if int(k[6]) < now_ms]

    if not closed:
        return None

    k = closed[-1]

    return [
        int(k[0]),
        float(k[1]),
        float(k[2]),
        float(k[3]),
        float(k[4]),
    ]


# =========================
# TELEGRAM
# =========================

async def send_telegram(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram variables missing.", flush=True)
        return

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
    }

    try:
        async with session.post(
            url,
            json=payload,
            timeout=20
        ) as r:
            body = await r.text()

            if r.status != 200:
                print("Telegram error:", r.status, body, flush=True)

    except Exception as e:
        print("Telegram exception:", e, flush=True)


def signal_text(symbol, new_dir, interval):
    name = symbol.upper()
    tf = interval.upper()

    if new_dir == -1:
        return (
            f"🟢 {name} — GREEN START\n"
            f"🔴 RED → 🟢 GREEN\n"
            f"⏱ Timeframe: {tf}\n"
            f"RSI Adjusted SuperTrend"
        )

    return (
        f"🔴 {name} — RED START\n"
        f"🟢 GREEN → 🔴 RED\n"
        f"⏱ Timeframe: {tf}\n"
        f"RSI Adjusted SuperTrend"
    )


# =========================
# MAIN SCANNER
# =========================

async def main():
    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(connector=connector) as session:

        print("Loading Binance USD-M perpetual symbols...", flush=True)

        symbols = await get_symbols(session)

        print(
            f"Found {len(symbols)} USDT perpetual symbols.",
            flush=True
        )

        histories = {}
        states = {}
        last_candle = {}

        # -------------------------
        # INITIAL LOAD
        # -------------------------

        total = len(symbols) * len(INTERVALS)
        done = 0

        for interval in INTERVALS:
            for sym in symbols:

                candles = await get_history(
                    session,
                    sym,
                    interval
                )

                if len(candles) >= 60:
                    try:
                        direction = calculate_direction(candles)

                        key = (sym, interval)

                        histories[key] = candles
                        states[key] = direction
                        last_candle[key] = candles[-1][0]

                    except Exception as e:
                        print(
                            "Init calculation error",
                            sym,
                            interval,
                            e,
                            flush=True
                        )

                done += 1

                if done % 50 == 0:
                    print(
                        f"Initialized {done}/{total}",
                        flush=True
                    )

        print(
            f"Scanner ready. States: {len(states)}",
            flush=True
        )

        await send_telegram(
            session,
            "✅ Binance SuperTrend Scanner Started\n"
            f"📊 Monitoring {len(symbols)} USDT Perpetuals\n"
            "⏱ Timeframes: 30M + 1H\n"
            "🔔 Alerts only when line color changes"
        )

        last_30m_scan = None
        last_1h_scan = None

        # -------------------------
        # LOOP
        # -------------------------

        while True:

            now = datetime.now(timezone.utc)

            current_30m_slot = (
                now.year,
                now.month,
                now.day,
                now.hour,
                now.minute // 30
            )

            current_1h_slot = (
                now.year,
                now.month,
                now.day,
                now.hour
            )

            # نعطي Binance ثواني بسيطة بعد إغلاق الشمعة
            if now.minute in (0, 30) and now.second >= 5:

                intervals_to_scan = []

                if current_30m_slot != last_30m_scan:
                    intervals_to_scan.append("30m")
                    last_30m_scan = current_30m_slot

                if (
                    now.minute == 0
                    and current_1h_slot != last_1h_scan
                ):
                    intervals_to_scan.append("1h")
                    last_1h_scan = current_1h_slot

                for interval in intervals_to_scan:

                    print(
                        f"Scanning closed {interval} candles...",
                        flush=True
                    )

                    checked = 0
                    signals = 0

                    for sym in symbols:

                        key = (sym, interval)

                        candle = await get_latest_closed(
                            session,
                            sym,
                            interval
                        )

                        if candle is None:
                            continue

                        if last_candle.get(key) == candle[0]:
                            continue

                        hist = histories.get(key)

                        if not hist:
                            hist = await get_history(
                                session,
                                sym,
                                interval
                            )

                        else:
                            hist.append(candle)
                            hist = hist[-HISTORY_LIMIT:]

                        if len(hist) < 60:
                            continue

                        try:
                            new_dir = calculate_direction(hist)

                        except Exception as e:
                            print(
                                "Calculation error",
                                sym,
                                interval,
                                e,
                                flush=True
                            )
                            continue

                        old_dir = states.get(key)

                        histories[key] = hist
                        last_candle[key] = candle[0]
                        states[key] = new_dir

                        checked += 1

                        # أهم شرط:
                        # يرسل فقط إذا تغير اللون
                        if (
                            old_dir is not None
                            and new_dir != old_dir
                        ):
                            signals += 1

                            text = signal_text(
                                sym,
                                new_dir,
                                interval
                            )

                            print(
                                "SIGNAL:",
                                text.replace("\n", " | "),
                                flush=True
                            )

                            await send_telegram(
                                session,
                                text
                            )

                    print(
                        f"Finished {interval}: "
                        f"{checked} checked, "
                        f"{signals} signals",
                        flush=True
                    )

            await asyncio.sleep(5)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("MAIN ERROR:", e, flush=True)
            time.sleep(10)
