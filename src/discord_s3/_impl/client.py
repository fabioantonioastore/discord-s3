import uuid
import asyncio
import pathlib
from collections.abc import AsyncIterator
import io
import contextlib

import aiofiles
import aiofiles.os
import discord

from .bot import Bot

INTENTS = discord.Intents(guilds=True, message_content=True)
CHUNK_SIZE = 20 * (1024**2)


class Client:
    def __init__(self, guild_id: int) -> None:
        self.__guild_id = guild_id
        self.__bots: dict[uuid.UUID, Bot] = dict()
        self.__bots_connect_tasks: dict[uuid.UUID, asyncio.Task[None]] = dict()
        self.__lock: asyncio.Lock = asyncio.Lock()

    @property
    def bots(self) -> dict[uuid.UUID, Bot]:
        return self.__bots

    @property
    def guild_id(self) -> int:
        return self.__guild_id

    async def add_bot(self, token: str) -> None:
        bot = Bot(intents=INTENTS)
        await bot.login(token=token)
        bot_connect_task = asyncio.create_task(coro=bot.connect())
        await bot.wait_until_ready()
        async with self.__lock:
            await bot.init(guild_id=self.guild_id)
        bot_id = uuid.uuid4()
        self.bots[bot_id] = bot
        self.__bots_connect_tasks[bot_id] = bot_connect_task

    def __get_free_bot(self) -> Bot:
        last_bot = None
        for bot in self.bots.values():
            if bot.is_able_to_make_request():
                return bot
            last_bot = bot
        if last_bot is None:
            raise RuntimeError("No bot available")
        return last_bot

    async def __upload(self, file: bytes, content: str | None = None) -> int:
        bot = self.__get_free_bot()
        message = await bot.upload(file=file, content=content)
        return message.id

    async def __upload_by_path(self, file: str | pathlib.Path) -> int:
        message_id = 0
        async with aiofiles.open(file=file, mode="rb") as opened_file:
            position = await opened_file.seek(0, io.SEEK_END)
            while position != 0:
                read_size = min(CHUNK_SIZE, position)
                position -= read_size
                await opened_file.seek(position)
                buffer = await opened_file.read(read_size)
                if message_id == 0:
                    message_id = await self.__upload(file=buffer)
                    continue
                message_id = await self.__upload(file=buffer, content=str(message_id))
        if message_id == 0:
            raise RuntimeError("File is empty")
        return message_id

    async def __upload_by_bytes_io(self, file: io.BytesIO) -> int:
        message_id = 0
        position = file.seek(0, io.SEEK_END)
        while position != 0:
            read_size = min(CHUNK_SIZE, position)
            position -= read_size
            file.seek(position)
            buffer = file.read(read_size)
            if message_id == 0:
                message_id = await self.__upload(file=buffer)
                continue
            message_id = await self.__upload(file=buffer, content=str(message_id))
        if message_id == 0:
            raise RuntimeError("File is empty")
        return message_id

    async def __upload_by_bytes(self, file: bytes) -> int:
        if 0 < len(file) <= CHUNK_SIZE:
            return await self.__upload(file=file)
        bytes_size = len(file)
        message_id = 0
        while bytes_size != 0:
            read_size = min(CHUNK_SIZE, bytes_size)
            buffer = file[bytes_size - read_size : bytes_size]
            bytes_size -= read_size
            if message_id == 0:
                message_id = await self.__upload(file=buffer)
                continue
            message_id = await self.__upload(file=buffer, content=str(message_id))
        if message_id == 0:
            raise RuntimeError("File is empty")
        return message_id

    async def __upload_by_async_iterator(self, file: AsyncIterator[bytes]) -> int:
        message_id = 0
        async with aiofiles.tempfile.TemporaryFile() as temporary_file:
            async for data in file:
                await temporary_file.write(data)
            position = await temporary_file.seek(0, io.SEEK_END)
            while position != 0:
                read_size = min(CHUNK_SIZE, position)
                position -= read_size
                await temporary_file.seek(position)
                buffer = await temporary_file.read(read_size)
                if message_id == 0:
                    message_id = await self.__upload(file=buffer)
                    continue
                message_id = await self.__upload(file=buffer, content=str(message_id))
        if message_id == 0:
            raise RuntimeError("File is empty")
        return message_id

    async def upload(
        self, file: bytes | pathlib.Path | str | AsyncIterator[bytes] | io.BytesIO
    ) -> int:
        match file:
            case AsyncIterator():
                return await self.__upload_by_async_iterator(file=file)
            case str() | pathlib.Path():
                return await self.__upload_by_path(file=file)
            case bytes():
                return await self.__upload_by_bytes(file=file)
            case io.BytesIO():
                return await self.__upload_by_bytes_io(file=file)
            case _:
                raise RuntimeError("Not valid file input")

    async def __get_message(self, message_id: int) -> discord.Message:
        bot = self.__get_free_bot()
        return await bot.get_message(message_id=message_id)

    async def download_in_memory(self, file_id: int) -> io.BytesIO:
        file = io.BytesIO()
        async for chunk in self.stream_download(file_id=file_id):
            file.write(chunk)
        file.seek(0)
        return file

    async def download(
        self, file_id: int, destination_path: str | pathlib.Path
    ) -> None:
        destination = pathlib.Path(destination_path)
        temporary_path = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.part"
        )
        try:
            async with aiofiles.open(file=temporary_path, mode="wb") as file:
                async for chunk in self.stream_download(file_id=file_id):
                    await file.write(chunk)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                await aiofiles.os.remove(temporary_path)
            raise
        await aiofiles.os.replace(temporary_path, destination)

    async def stream_download(self, file_id: int) -> AsyncIterator[bytes]:
        while True:
            message = await self.__get_message(message_id=file_id)
            attachment = message.attachments[0]
            yield await attachment.read()
            if not message.content.isnumeric():
                return
            file_id = int(message.content)
