import asyncio
import io

import discord

GLOBAL_REQUESTS_RESET_TIME = 1
MAX_GLOBAL_REQUESTS = 50
STORAGE_CHANNEL_NAME = "storage"


class Bot(discord.Client):
    def __init__(self, intents: discord.Intents) -> None:
        super().__init__(intents=intents)
        self.__global_requests = 0
        self.__global_requests_reset_task = asyncio.create_task(
            coro=self.__global_requests_reset()
        )

    async def __global_requests_reset(self) -> None:
        while True:
            self.__global_requests = 0
            await asyncio.sleep(GLOBAL_REQUESTS_RESET_TIME)

    def __increment_global_requests(self) -> None:
        self.__global_requests += 1

    def is_able_to_make_request(self) -> bool:
        return self.__global_requests < MAX_GLOBAL_REQUESTS

    async def init(self, guild_id: int) -> None:
        self.__guild = self.get_guild(guild_id)
        if self.__guild is None:
            raise RuntimeError("Guild not found")
        for channel in self.__guild.channels:
            if (
                channel.name == STORAGE_CHANNEL_NAME
                and channel.type == discord.ChannelType.text
            ):
                self.__storage_channel = channel
                return
        self.__increment_global_requests()
        self.__storage_channel = await self.__guild.create_text_channel(
            name=STORAGE_CHANNEL_NAME
        )

    async def async_close(self) -> None:
        self.__global_requests_reset_task.cancel()
        await self.close()

    async def upload(self, file: bytes, content: str | None) -> discord.Message:
        discord_file = discord.File(fp=io.BytesIO(file))
        self.__increment_global_requests()
        return await self.__storage_channel.send(content=content, file=discord_file)

    async def get_message(self, message_id: int) -> discord.Message:
        self.__increment_global_requests()
        return await self.__storage_channel.fetch_message(message_id)
