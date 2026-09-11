#!/usr/bin/env python3
# Binance RSI-adjusted SuperTrend -> Telegram scanner
# 1H Binance USD-M Futures | RED->GREEN / GREEN->RED only
#
# IMPORTANT:
# - Put your NEW Telegram bot token in TELEGRAM_BOT_TOKEN below.
# - Do NOT send the token to anyone.
# - CHAT_ID is already set to the ID you provided.
# - This program uses public Binance market data only; no Binance API key is needed.
import os
import asyncio
import json
import math
import time
from datetime import datetime, timezone

import aiohttp
import websockets
import os

TELEGRAM_BOT_TOKEN = "".join(os.getenv("TELEGRAM_BOT_TOKEN", "").split())
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FACTOR = 2.0
ATR_LEN = 20
RSI_LEN = 14
DIVIDE_RSI_BY = 100.0

BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/stream"

HISTORY_LIMIT = 120
STREAM_CHUNK = 200


def rma(values, length):
    """TradingView ta.rma equivalent: SMA seed, then Wilder RMA."""
    out = [math.nan] * len(values)
    if len(values) < length:
        return out
    window = [x for x in values[:length] if not math.isnan(x)]
    if len(window) < length:
        return out
    prev = sum(window) / length
    out[length - 1] = prev
    alpha = 1.0 / length
    for i in range(length, len(values)):
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
        d = closes[i] - closes[i - 1]
        gains[i] = max(d, 0.0)
        losses[i] = max(-d, 0.0)

    # Pine ta.rma ignores the first na and seeds after length observations.
    gain_vals = gains[1:]
    loss_vals = losses[1:]
    rg = rma(gain_vals, length)
    rl = rma(loss_vals, length)

    rsi = [math.nan] * len(closes)
    for j in range(len(rg)):
        i = j + 1
        if math.isnan(rg[j]) or math.isnan(rl[j]):
            continue
        if rl[j] == 0:
            rsi[i] = 100.0
        elif rg[j] == 0:
            rsi[i] = 0.0
        else:
            rs = rg[j] / rl[j]
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def pine_atr(highs, lows, closes, length):
    tr = [math.nan] * len(closes)
    for i in range(len(closes)):
        if i == 0:
            tr[i] = highs[i] - lows[i]
        else:
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
    return rma(tr, length)


def calculate_direction(candles):
    """
    Reproduces the direction logic of the supplied Pine script.
    candles = list of [open_time, open, high, low, close]
    Returns -1 for green, +1 for red, plus supertrend values.
    """
    n = len(candles)
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    hl2 = [(h + l) / 2.0 for h, l in zip(highs, lows)]

    atr = pine_atr(highs, lows, closes, ATR_LEN)
    rsi = pine_rsi(closes, RSI_LEN)

    lower = [math.nan] * n
    upper = [math.nan] * n
    lower2 = [math.nan] * n
    upper2 = [math.nan] * n
    direction = [math.nan] * n
    direction2 = [math.nan] * n
    superdir = [math.nan] * n
    superdir2 = [math.nan] * n

    for i in range(n):
        if math.isnan(atr[i]):
            continue

        src = hl2[i]
        up = src + FACTOR * atr[i]
        lo = src - FACTOR * atr[i]

        a_rsi = 0.0
        if not math.isnan(rsi[i]):
            a_rsi = abs(rsi[i] - 50.0) / DIVIDE_RSI_BY

        up2 = up + up * a_rsi
        lo2 = lo - lo * a_rsi

        if i == 0:
            prev_lower = 0.0
            prev_upper = 0.0
            prev_lower2 = 0.0
            prev_upper2 = 0.0
            prev_super = math.nan
            prev_super2 = math.nan
        else:
            prev_lower = lower[i - 1] if not math.isnan(lower[i - 1]) else 0.0
            prev_upper = upper[i - 1] if not math.isnan(upper[i - 1]) else 0.0
            prev_lower2 = lower2[i - 1] if not math.isnan(lower2[i - 1]) else 0.0
            prev_upper2 = upper2[i - 1] if not math.isnan(upper2[i - 1]) else 0.0
            prev_super = superdir[i - 1]
            prev_super2 = superdir2[i - 1]

        prev_close = closes[i - 1] if i > 0 else math.nan

        # Pine:
        # lower := lower > prevLower or close[1] < prevLower ? lower : prevLower
        # upper := upper < prevUpper or close[1] > prevUpper ? upper : prevUpper
        if i == 0 or lo > prev_lower or prev_close < prev_lower:
            lower[i] = lo
        else:
            lower[i] = prev_lower

        if i == 0 or up < prev_upper or prev_close > prev_upper:
            upper[i] = up
        else:
            upper[i] = prev_upper

        # RSI-adjusted bands
        if i == 0 or lo2 > prev_lower2 or prev_close < prev_lower2:
            lower2[i] = lo2
        else:
            lower2[i] = prev_lower2

        if i == 0 or up2 < prev_upper2 or prev_close > prev_upper2:
            upper2[i] = up2
        else:
            upper2[i] = prev_upper2

        # Pine initializes direction to 1 on first valid ATR bar.
        if i == 0 or math.isnan(atr[i - 1]):
            direction[i] = 1
            direction2[i] = 1
        else:
            if prev_super == prev_upper:
                direction[i] = -1 if closes[i] > upper[i] else 1
            else:
                direction[i] = 1 if closes[i] < lower[i] else -1

            if prev_super2 == prev_upper2:
                direction2[i] = -1 if closes[i] > upper2[i] else 1
            else:
                direction2[i] = 1 if closes[i] < lower2[i] else -1

        superdir[i] = lower[i] if direction[i] == -1 else upper[i]
        superdir2[i] = lower2[i] if direction2[i] == -1 else upper2[i]

    return direction2[-1], superdir2[-1]


async def get_symbols(session):
    async with session.get(f"{BINANCE_REST}/fapi/v1/exchangeInfo", timeout=30) as r:
        r.raise_for_status()
        data = await r.json()

    symbols = []
    for s in data["symbols"]:
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
        ):
            symbols.append(s["symbol"].lower())
    return sorted(symbols)


async def get_history(session, symbol, interval):
    url = f"{BINANCE_REST}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": HISTORY_LIMIT}
    async with session.get(url, params=params, timeout=30) as r:
        r.raise_for_status()
        raw = await r.json()

    candles = []
    now_ms = int(time.time() * 1000)
    for k in raw:
        close_time = int(k[6])
        # Ignore the currently-open candle. We only signal on confirmed candles.
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


async def send_telegram(session, text):
    if not TELEGRAM_BOT_TOKEN or "PUT_YOUR" in TELEGRAM_BOT_TOKEN:
        print("Telegram token is not configured. Signal:", text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    async with session.post(url, json=payload, timeout=20) as r:
        body = await r.text()
        if r.status != 200:
            print("Telegram error:", r.status, body)


def signal_text(symbol, new_dir, interval):
    name = symbol.upper()
    if new_dir == -1:
        return f"🟢 {name} — GREEN START\nRSI Adjusted SuperTrend\n⏱ {interval.upper()}"
    return f"🔴 {name} — RED START\nRSI Adjusted SuperTrend\n⏱ {interval.upper()}"


async def main():
    if "PUT_YOUR_NEW" in TELEGRAM_BOT_TOKEN:
        print("\nضع توكن Telegram الجديد داخل TELEGRAM_BOT_TOKEN أولاً.\n")
        return

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        print("Loading Binance USD-M perpetual symbols...")
        symbols = await get_symbols(session)
        print(f"Found {len(symbols)} USDT perpetual symbols.")
        INTERVALS = ["1h", "30m"]
        states = {}
        histories = {}

        # Initial history: establish the current direction without sending an alert.
        sem = asyncio.Semaphore(20)

        async def load_one(sym, interval):
            async with sem:
                try:
                    candles = await get_history(session, sym, interval)
                    if len(candles) >= 60:
                        d, _ = calculate_direction(candles)
                        histories[(sym, interval)] = candles
                        states[(sym, interval)] = int(d)
                except Exception as e:
                    print("History error", sym, e)

        await asyncio.gather(*(load_one(s, interval) for interval in INTERVALS for s in symbols))
        print(f"Initialized {len(symbols)} symbols. Waiting for confirmed 1H + 30M candles...")
        await send_telegram(session, f"✅ Binance SuperTrend Scanner Started\n📊 Monitoring {len(symbols)} USDT Perpetuals\n⏱ Timeframes: 1H + 30M")
        # Split streams so each WebSocket URL stays reasonably small.
        chunks = [symbols[i:i + STREAM_CHUNK] for i in range(0, len(symbols), STREAM_CHUNK)]

        async def stream_worker(chunk):
            streams = "/".join(
    f"{s}@kline_{interval}"
    for s in chunk
    for interval in INTERVALS
)
            url = f"{BINANCE_WS}?streams={streams}"

            while True:
                try:
                    async with websockets.connect(
                        url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                        max_size=2**20,
                    ) as ws:
                        print(f"WebSocket connected: {len(chunk)} symbols")
                        async for raw in ws:
                            msg = json.loads(raw)
                            data = msg.get("data", {})
                            if data.get("e") != "kline":
                                continue

                            k = data["k"]
                            if not k.get("x"):  # candle not closed
                                continue

                            sym = k["s"].lower()
                            interval = k["i"]
                            candle = [
                                int(k["t"]),
                                float(k["o"]),
                                float(k["h"]),
                                float(k["l"]),
                                float(k["c"]),
                            ]

                            hist = histories.get((sym, interval), [])
                            if hist and hist[-1][0] == candle[0]:
                                hist[-1] = candle
                            else:
                                hist.append(candle)

                            histories[(sym, interval)] = hist[-HISTORY_LIMIT:]

                            if len(hist) < 60:
                                continue

                            new_dir, _ = calculate_direction(hist)
                            new_dir = int(new_dir)
                            old_dir = states.get((sym, interval))

                            if old_dir is None:
                                states[(sym, interval)] = new_dir
                                continue

                            if new_dir != old_dir:
                                states[(sym, interval)] = new_dir
                                text = signal_text(sym, new_dir, interval)
                                print(datetime.now(timezone.utc).isoformat(), text)
                                await send_telegram(session, text)

                except Exception as e:
                    print("WebSocket disconnected:", e)
                    await asyncio.sleep(5)

        await asyncio.gather(*(stream_worker(c) for c in chunks))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
