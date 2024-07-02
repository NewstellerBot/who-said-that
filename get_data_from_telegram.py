import sys
import asyncio
import json
from telethon import TelegramClient
import os

api_id = os.environ["API_ID"]
api_hash = os.environ["API_HASH"]


async def main():
    client = TelegramClient("cs156", api_id, api_hash)
    try:
        await client.connect()
    except:
        print("Couldn't connect to Telegram")
        sys.exit(1)

    data = []
    async for message in client.iter_messages(-698440036):
        print(message)
        m = {
            "id": message.id,
            "text": message.text,
            "from": message.from_id.user_id,
            "timestamp": message.date.timestamp(),
        }
        data.append(m)

    with open("test.json", "w") as f:
        f.write(json.dumps(data))


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except Exception as e:
        print("error while running the program: ", e)
