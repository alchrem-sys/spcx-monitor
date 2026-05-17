import os
import json
import asyncio
import aiohttp
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("spcx")

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
COIN = os.environ.get("COIN", "SPCX")

HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_API = "https://api.hyperliquid.xyz/info"

listing_notified = False
tracking_coin = None
tracking_active = False
last_update_id = 0
user_chat_id = None


async def send_telegram(session: aiohttp.ClientSession, text: str, chat_id: str = None):
    cid = chat_id or user_chat_id
    if not cid:
        log.warning("No chat_id yet — skipping message")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": cid, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as r:
            log.info(f"TG sent: {r.status}")
    except Exception as e:
        log.error(f"TG error: {e}")


async def spam_listing_alert(session: aiohttp.ClientSession, price_info: str):
    global listing_notified
    if listing_notified:
        return
    listing_notified = True
    msg = (
        f"🚨🚨🚨 <b>ТОРГИ {COIN} ВІДКРИЛИСЬ!</b> 🚨🚨🚨\n\n"
        f"{price_info}\n\n"
        f"👉 <a href='https://app.hyperliquid.xyz/trade/xyz:{COIN}'>ВІДКРИТИ HYPERLIQUID</a>\n\n"
        f"⚡ ЛОНГ ЗАРАЗ!"
    )
    tasks = [send_telegram(session, msg) for _ in range(5)]
    await asyncio.gather(*tasks)
    log.info("LISTING ALERT x5!")


def parse_book(data) -> str | None:
    if not data or not isinstance(data, dict):
        return None
    levels = data.get("levels", [])
    if not levels:
        return None
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    if not bids and not asks:
        return None
    info = ""
    if bids:
        info += f"Bid: {bids[0]['px']} ({bids[0]['sz']})"
    if asks:
        info += f" | Ask: {asks[0]['px']} ({asks[0]['sz']})"
    return info


def get_mid_price(data) -> str | None:
    if not data or not isinstance(data, dict):
        return None
    levels = data.get("levels", [])
    if not levels:
        return None
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    if bids and asks:
        mid = (float(bids[0]["px"]) + float(asks[0]["px"])) / 2
        return f"${mid:,.2f}"
    if bids:
        return f"${float(bids[0]['px']):,.2f}"
    if asks:
        return f"${float(asks[0]['px']):,.2f}"
    return None


# --- Telegram command polling ---
async def poll_commands(session: aiohttp.ClientSession):
    global last_update_id, tracking_coin, tracking_active, user_chat_id
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"

    while True:
        try:
            params = {"offset": last_update_id + 1, "timeout": 5}
            async with session.get(url, params=params) as r:
                data = await r.json()
                for update in data.get("result", []):
                    last_update_id = update["update_id"]
                    msg = update.get("message", {})
                    text = msg.get("text", "")
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if not user_chat_id:
                        user_chat_id = chat_id
                        log.info(f"Chat ID set: {chat_id}")

                    if text.startswith("/start"):
                        user_chat_id = chat_id
                        await send_telegram(session,
                            f"✅ <b>Бот активний!</b>\n\n"
                            f"🔍 Моніторю лістинг: <b>{COIN}</b>\n\n"
                            f"/track BTC — трекати ціну кожну секунду\n"
                            f"/stop — зупинити трекінг\n"
                            f"/status — статус бота", chat_id)

                    elif text.startswith("/track "):
                        coin = text.split(" ", 1)[1].strip().upper()
                        tracking_coin = coin
                        tracking_active = True
                        await send_telegram(session, f"📡 Трекаю <b>{coin}</b> — ціна кожну секунду", chat_id)
                        log.info(f"Tracking started: {coin}")

                    elif text.startswith("/stop"):
                        tracking_active = False
                        tracking_coin = None
                        await send_telegram(session, "⏹ Трекінг зупинено", chat_id)
                        log.info("Tracking stopped")

                    elif text.startswith("/status"):
                        if tracking_active:
                            await send_telegram(session, f"📡 Трекаю: <b>{tracking_coin}</b>\n🔍 Моніторю лістинг: <b>{COIN}</b>", chat_id)
                        else:
                            await send_telegram(session, f"⏹ Трекінг неактивний\n🔍 Моніторю лістинг: <b>{COIN}</b>", chat_id)
        except Exception as e:
            log.error(f"Poll commands error: {e}")
        await asyncio.sleep(2)


# --- Price tracker (every 1s) ---
async def price_tracker(session: aiohttp.ClientSession):
    while True:
        if tracking_active and tracking_coin:
            try:
                payload = {"type": "l2Book", "coin": tracking_coin}
                async with session.post(HL_API, json=payload) as r:
                    data = await r.json()
                    price = get_mid_price(data)
                    book = parse_book(data)
                    if price:
                        await send_telegram(session, f"💰 <b>{tracking_coin}</b>: {price}\n{book}")
                    else:
                        await send_telegram(session, f"❌ <b>{tracking_coin}</b>: немає даних (стакан порожній)")
            except Exception as e:
                log.error(f"Track error: {e}")
        await asyncio.sleep(1)


# --- SPCX listing monitor (WebSocket) ---
async def ws_monitor(session: aiohttp.ClientSession):
    while not listing_notified:
        try:
            async with session.ws_connect(HL_WS) as ws:
                sub = {"method": "subscribe", "subscription": {"type": "l2Book", "coin": COIN}}
                await ws.send_json(sub)
                log.info(f"WS subscribed to {COIN} l2Book")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        book = data.get("data", {})
                        price_info = parse_book(book)
                        if price_info:
                            log.info(f"ORDER BOOK DETECTED: {price_info}")
                            await spam_listing_alert(session, price_info)
                            return
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except Exception as e:
            log.error(f"WS error: {e}")
        await asyncio.sleep(3)


# --- SPCX listing monitor (HTTP fallback) ---
async def http_poll(session: aiohttp.ClientSession):
    while not listing_notified:
        try:
            payload = {"type": "l2Book", "coin": COIN}
            async with session.post(HL_API, json=payload) as r:
                data = await r.json()
                price_info = parse_book(data)
                if price_info:
                    log.info(f"HTTP — ORDER BOOK DETECTED: {price_info}")
                    await spam_listing_alert(session, price_info)
                    return
        except Exception as e:
            log.error(f"HTTP poll error: {e}")
        await asyncio.sleep(1)


async def main():
    log.info(f"Bot started | Listing monitor: {COIN} | Send /start to the bot in Telegram")
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            ws_monitor(session),
            http_poll(session),
            poll_commands(session),
            price_tracker(session),
        )
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
