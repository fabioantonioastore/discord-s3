import asyncio
import os

from dotenv import load_dotenv
from discord_s3 import Client

load_dotenv()
GUILD_ID = int(os.getenv("DISCORD_STORAGE_SERVER_ID"))  # type: ignore
TOKEN = str(os.getenv("DISCORD_BOT_TOKEN"))
ACTUAL_DIRECTORY = os.path.dirname(p=os.path.abspath(__file__))


async def main() -> None:
    s3 = Client(guild_id=GUILD_ID)
    await s3.add_bot(token=TOKEN)
    file_id = await s3.upload(file=ACTUAL_DIRECTORY + "/text_file.txt")
    await s3.download(file_id=file_id, destination_path=ACTUAL_DIRECTORY + "/out.txt")


asyncio.run(main=main())
