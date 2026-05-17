import os
import json
import time
import asyncio
import aiohttp
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("spcx")

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
COIN = os.environ.get("COIN", "SPCX")

HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_API = "https://api.hyperliquid.xyz/info"

notified = False


async def send_telegram(session: aiohttp.ClientSession, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as r:
            log.info(f"Telegram sent: {r.status}")
    except Exception as e:
        log.error(f"Telegram error: {e}")


async def spam_telegram(session: aiohttp.ClientSession, price_info: str):
    global notified
    if notified:
        return
    notified = True

    msg = (
        f"🚨🚨🚨 <b>ТОРГИ {COIN} ВІДКРИЛИСЬ!</b> 🚨🚨🚨\n\n"
        f"{price_info}\n\n"
        f"👉 <a href='https://app.hyperliquid.xyz/trade/xyz:{COIN}'>ВІДКРИТИ HYPERLIQUID</a>\n\n"
        f"⚡ ЛОНГ ЗАРАЗ!"
    )
    tasks = [send_telegram(session, msg) for _ in range(5)]
    await asyncio.gather(*tasks)
    log.info("ALERT SENT x5!")


def parse_book(data) -> str | None:
    levels = data.get("levels", [])
    if not levels:
        return None
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    if not bids and not asks:
        return None
    info = ""
    if bids:
        info += f"Best Bid: {bids[0]['px']} ({bids[0]['sz']})"
    if asks:
        info += f" | Best Ask: {asks[0]['px']} ({asks[0]['sz']})"
    return info


async def ws_monitor(session: aiohttp.ClientSession):
    while not notified:
        try:
            async with session.ws_connect(HL_WS) as ws:
                sub = {
                    "method": "subscribe",
                    "subscription": {"type": "l2Book", "coin": COIN},
                }
                await ws.send_json(sub)
                log.info(f"WebSocket subscribed to {COIN} l2Book")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        book = data.get("data", {})
                        price_info = parse_book(book)
                        if price_info:
                            log.info(f"ORDER BOOK DETECTED: {price_info}")
                            await spam_telegram(session, price_info)
                            return
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        log.warning("WebSocket closed/error, reconnecting...")
                        break
        except Exception as e:
            log.error(f"WebSocket error: {e}")
        await asyncio.sleep(1)


async def http_poll(session: aiohttp.ClientSession):
    while not notified:
        try:
            payload = {"type": "l2Book", "coin": COIN}
            async with session.post(HL_API, json=payload) as r:
                data = await r.json()
                price_info = parse_book(data)
                if price_info:
                    log.info(f"HTTP POLL — ORDER BOOK DETECTED: {price_info}")
                    await spam_telegram(session, price_info)
                    return
                else:
                    log.info("HTTP poll — book empty")
        except Exception as e:
            log.error(f"HTTP poll error: {e}")
        await asyncio.sleep(1)


async def main():
    log.info(f"Starting SPCX monitor for {COIN}")
    log.info("Dual mode: WebSocket + HTTP polling every 1s")

    async with aiohttp.ClientSession() as session:
        await send_telegram(session, f"✅ Бот запущено! Моніторю {COIN} на Hyperliquid.\nWebSocket + HTTP polling кожну секунду.")

        await asyncio.gather(
            ws_monitor(session),
            http_poll(session),
        )

        await send_telegram(session, f"✅ Моніторинг {COIN} завершено — торги відкрились!")
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
