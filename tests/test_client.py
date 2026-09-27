import asyncio
import io
import itertools
import os
import pathlib
import uuid
from collections.abc import AsyncIterator, Coroutine
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from discord_s3 import Client
from discord_s3._impl import client as client_module


def run[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


class FakeBot:
    _ids = itertools.count(1000)

    def __init__(self, available: bool = True) -> None:
        self.available = available
        self.messages: dict[int, tuple[bytes, str | None]] = {}
        self.calls: list[tuple[bytes, str | None]] = []

    def is_able_to_make_request(self) -> bool:
        return self.available

    async def upload(self, file: bytes, content: str | None) -> SimpleNamespace:
        message_id = next(self._ids)
        self.messages[message_id] = (file, content)
        self.calls.append((file, content))
        return SimpleNamespace(id=message_id)

    async def get_message(self, message_id: int) -> SimpleNamespace:
        if message_id not in self.messages:
            response = MagicMock(status=404, reason="Not Found")
            raise discord.NotFound(response, "Unknown Message")
        file, content = self.messages[message_id]
        # Discord returns "" as the content of a message sent without one
        return SimpleNamespace(
            content=content or "",
            attachments=[SimpleNamespace(read=AsyncMock(return_value=file))],
        )


def rebuild(bot: FakeBot, head_id: int) -> bytes:
    data = b""
    message_id: int | None = head_id
    while message_id is not None:
        chunk, next_id = bot.messages[message_id]
        data += chunk
        message_id = int(next_id) if next_id is not None else None
    return data


async def agen(pieces: list[bytes]) -> AsyncIterator[bytes]:
    for piece in pieces:
        await asyncio.sleep(0)
        yield piece


@pytest.fixture
def small_chunks(monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(client_module, "CHUNK_SIZE", 3)
    return 3


@pytest.fixture
def client_with_bot() -> tuple[Client, FakeBot]:
    client = Client(guild_id=123)
    bot = FakeBot()
    client.bots[uuid.uuid4()] = bot  # type: ignore[assignment]
    return client, bot


def test_guild_id_returns_constructor_value() -> None:
    assert Client(guild_id=42).guild_id == 42


def test_bots_starts_empty() -> None:
    assert Client(guild_id=42).bots == {}


def test_upload_without_bots_raises_no_bot_available() -> None:
    with pytest.raises(RuntimeError, match="No bot available"):
        run(Client(guild_id=1).upload(file=b"data"))


def test_upload_uses_first_available_bot() -> None:
    client = Client(guild_id=1)
    busy, free, other = FakeBot(available=False), FakeBot(), FakeBot()
    for bot in (busy, free, other):
        client.bots[uuid.uuid4()] = bot  # type: ignore[assignment]

    run(client.upload(file=b"data"))

    assert busy.calls == []
    assert free.calls == [(b"data", None)]
    assert other.calls == []


def test_upload_falls_back_to_last_bot_when_all_are_busy() -> None:
    client = Client(guild_id=1)
    first, last = FakeBot(available=False), FakeBot(available=False)
    for bot in (first, last):
        client.bots[uuid.uuid4()] = bot  # type: ignore[assignment]

    run(client.upload(file=b"data"))

    assert first.calls == []
    assert last.calls == [(b"data", None)]


@pytest.mark.parametrize("file", [bytearray(b"data"), 123, None, ["a"]])
def test_upload_with_unsupported_type_raises_not_valid_file_input(
    client_with_bot: tuple[Client, FakeBot], file: Any
) -> None:
    client, bot = client_with_bot

    with pytest.raises(RuntimeError, match="Not valid file input"):
        run(client.upload(file=file))
    assert bot.calls == []


def test_upload_bytes_single_chunk_sends_one_message_without_content(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int
) -> None:
    client, bot = client_with_bot

    head_id = run(client.upload(file=b"abc"))

    assert bot.calls == [(b"abc", None)]
    assert rebuild(bot, head_id) == b"abc"


@pytest.mark.parametrize(
    "data",
    [b"abcd", b"abcdef", b"abcdefghij", os.urandom(1000)],
    ids=["one-extra-byte", "exact-multiple", "uneven", "random-1000"],
)
def test_upload_bytes_multi_chunk_chains_from_end(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int, data: bytes
) -> None:
    client, bot = client_with_bot

    head_id = run(client.upload(file=data))

    assert rebuild(bot, head_id) == data
    assert all(len(chunk) <= small_chunks for chunk, _ in bot.calls)
    assert bot.calls[0] == (data[-len(bot.calls[0][0]) :], None)
    assert all(content is not None for _, content in bot.calls[1:])


def test_upload_empty_bytes_raises_file_is_empty(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int
) -> None:
    client, bot = client_with_bot

    with pytest.raises(RuntimeError, match="File is empty"):
        run(client.upload(file=b""))
    assert bot.calls == []


@pytest.mark.parametrize("as_str", [False, True], ids=["pathlib", "str"])
@pytest.mark.parametrize("data", [b"ab", b"abcdef", b"abcdefghij"])
def test_upload_path_chains_file_from_end(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
    as_str: bool,
    data: bytes,
) -> None:
    client, bot = client_with_bot
    path = tmp_path / "file.bin"
    path.write_bytes(data)

    head_id = run(client.upload(file=str(path) if as_str else path))

    assert rebuild(bot, head_id) == data
    assert all(len(chunk) <= small_chunks for chunk, _ in bot.calls)
    assert bot.calls[0][1] is None


def test_upload_empty_path_raises_file_is_empty(
    client_with_bot: tuple[Client, FakeBot], tmp_path: pathlib.Path
) -> None:
    client, bot = client_with_bot
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    with pytest.raises(RuntimeError, match="File is empty"):
        run(client.upload(file=path))
    assert bot.calls == []


def test_upload_missing_path_raises_file_not_found(
    client_with_bot: tuple[Client, FakeBot], tmp_path: pathlib.Path
) -> None:
    client, _ = client_with_bot

    with pytest.raises(FileNotFoundError):
        run(client.upload(file=tmp_path / "missing.bin"))


@pytest.mark.parametrize("data", [b"ab", b"abcdef", b"abcdefghij"])
def test_upload_bytes_io_chains_buffer_from_end(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int, data: bytes
) -> None:
    client, bot = client_with_bot

    head_id = run(client.upload(file=io.BytesIO(data)))

    assert rebuild(bot, head_id) == data
    assert all(len(chunk) <= small_chunks for chunk, _ in bot.calls)


def test_upload_bytes_io_ignores_current_position(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int
) -> None:
    client, bot = client_with_bot
    buffer = io.BytesIO(b"abcdefghij")
    buffer.seek(7)

    head_id = run(client.upload(file=buffer))

    assert rebuild(bot, head_id) == b"abcdefghij"


def test_upload_empty_bytes_io_raises_file_is_empty(
    client_with_bot: tuple[Client, FakeBot],
) -> None:
    client, bot = client_with_bot

    with pytest.raises(RuntimeError, match="File is empty"):
        run(client.upload(file=io.BytesIO()))
    assert bot.calls == []


@pytest.mark.parametrize(
    "pieces",
    [[b"a"], [b"abc"], [b"ab", b"cdefg", b"", b"h", b"ij"], [b"x" * 10]],
    ids=["one-byte", "exact-chunk", "irregular", "single-large-piece"],
)
def test_upload_async_iterator_chains_stream_from_end(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int, pieces: list[bytes]
) -> None:
    client, bot = client_with_bot

    head_id = run(client.upload(file=agen(pieces)))

    assert rebuild(bot, head_id) == b"".join(pieces)
    assert all(len(chunk) <= small_chunks for chunk, _ in bot.calls)


@pytest.mark.parametrize("pieces", [[], [b"", b""]], ids=["no-items", "empty-items"])
def test_upload_empty_async_iterator_raises_file_is_empty(
    client_with_bot: tuple[Client, FakeBot], pieces: list[bytes]
) -> None:
    client, bot = client_with_bot

    with pytest.raises(RuntimeError, match="File is empty"):
        run(client.upload(file=agen(pieces)))
    assert bot.calls == []


def test_add_bot_logs_in_connects_and_registers_bot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_bot = MagicMock()
    fake_bot.login = AsyncMock()
    fake_bot.connect = AsyncMock()
    fake_bot.wait_until_ready = AsyncMock()
    fake_bot.init = AsyncMock()
    bot_class = MagicMock(return_value=fake_bot)
    monkeypatch.setattr(client_module, "Bot", bot_class)
    client = Client(guild_id=777)

    async def scenario() -> None:
        await client.add_bot(token="token")
        await asyncio.sleep(0)

    run(scenario())

    bot_class.assert_called_once_with(intents=client_module.INTENTS)
    fake_bot.login.assert_awaited_once_with(token="token")
    fake_bot.connect.assert_awaited_once()
    fake_bot.wait_until_ready.assert_awaited_once()
    fake_bot.init.assert_awaited_once_with(guild_id=777)
    assert list(client.bots.values()) == [fake_bot]


def test_add_bot_propagates_init_failure_without_registering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_bot = MagicMock()
    fake_bot.login = AsyncMock()
    fake_bot.connect = AsyncMock()
    fake_bot.wait_until_ready = AsyncMock()
    fake_bot.init = AsyncMock(side_effect=RuntimeError("Guild not found"))
    monkeypatch.setattr(client_module, "Bot", MagicMock(return_value=fake_bot))
    client = Client(guild_id=777)

    with pytest.raises(RuntimeError, match="Guild not found"):
        run(client.add_bot(token="token"))
    assert client.bots == {}


ROUND_TRIP_DATA = pytest.mark.parametrize(
    "data",
    [b"a", b"abc", b"abcdefghij", os.urandom(1000)],
    ids=["one-byte", "exact-chunk", "uneven", "random-1000"],
)


async def collect(stream: AsyncIterator[bytes]) -> list[bytes]:
    return [chunk async for chunk in stream]


@ROUND_TRIP_DATA
def test_download_in_memory_returns_uploaded_bytes(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int, data: bytes
) -> None:
    client, _ = client_with_bot

    async def scenario() -> io.BytesIO:
        file_id = await client.upload(file=data)
        return await client.download_in_memory(file_id=file_id)

    assert run(scenario()).getvalue() == data


@pytest.mark.parametrize(
    "make_input",
    [
        lambda data, _: data,
        lambda data, _: io.BytesIO(data),
        lambda data, path: path,
        lambda data, path: str(path),
        lambda data, _: agen([data[:4], data[4:]]),
    ],
    ids=["bytes", "bytes-io", "pathlib", "str", "async-iterator"],
)
def test_download_in_memory_round_trips_every_upload_input_type(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
    make_input: Any,
) -> None:
    client, _ = client_with_bot
    data = b"abcdefghij"
    source = tmp_path / "source.bin"
    source.write_bytes(data)

    async def scenario() -> io.BytesIO:
        file_id = await client.upload(file=make_input(data, source))
        return await client.download_in_memory(file_id=file_id)

    assert run(scenario()).getvalue() == data


def test_download_in_memory_returns_buffer_positioned_at_start(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int
) -> None:
    client, _ = client_with_bot

    async def scenario() -> io.BytesIO:
        file_id = await client.upload(file=b"abcdefghij")
        return await client.download_in_memory(file_id=file_id)

    buffer = run(scenario())

    assert buffer.tell() == 0
    assert buffer.read() == b"abcdefghij"


def test_download_in_memory_with_unknown_id_raises_not_found(
    client_with_bot: tuple[Client, FakeBot],
) -> None:
    client, _ = client_with_bot

    with pytest.raises(discord.NotFound):
        run(client.download_in_memory(file_id=1))


def test_download_in_memory_without_bots_raises_no_bot_available() -> None:
    with pytest.raises(RuntimeError, match="No bot available"):
        run(Client(guild_id=1).download_in_memory(file_id=1))


@pytest.mark.parametrize("as_str", [False, True], ids=["pathlib", "str"])
@ROUND_TRIP_DATA
def test_download_writes_uploaded_bytes_to_destination(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
    as_str: bool,
    data: bytes,
) -> None:
    client, _ = client_with_bot
    destination = tmp_path / "out.bin"

    async def scenario() -> None:
        file_id = await client.upload(file=data)
        path = str(destination) if as_str else destination
        await client.download(file_id=file_id, destination_path=path)

    run(scenario())

    assert destination.read_bytes() == data


def test_download_overwrites_existing_destination(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
) -> None:
    client, _ = client_with_bot
    destination = tmp_path / "out.bin"
    destination.write_bytes(b"previous content that is longer")

    async def scenario() -> None:
        file_id = await client.upload(file=b"abcdefg")
        await client.download(file_id=file_id, destination_path=destination)

    run(scenario())

    assert destination.read_bytes() == b"abcdefg"


@ROUND_TRIP_DATA
def test_stream_download_yields_chunks_in_file_order(
    client_with_bot: tuple[Client, FakeBot], small_chunks: int, data: bytes
) -> None:
    client, bot = client_with_bot

    async def scenario() -> list[bytes]:
        file_id = await client.upload(file=data)
        return await collect(client.stream_download(file_id=file_id))

    chunks = run(scenario())

    assert b"".join(chunks) == data
    assert len(chunks) == len(bot.calls)
    assert all(0 < len(chunk) <= small_chunks for chunk in chunks)


def test_stream_download_with_unknown_id_raises_not_found(
    client_with_bot: tuple[Client, FakeBot],
) -> None:
    client, _ = client_with_bot

    with pytest.raises(discord.NotFound):
        run(collect(client.stream_download(file_id=1)))


@pytest.mark.parametrize(
    "download",
    [
        lambda client, file_id, _: client.download_in_memory(file_id=file_id),
        lambda client, file_id, path: client.download(
            file_id=file_id, destination_path=path
        ),
        lambda client, file_id, _: collect(client.stream_download(file_id=file_id)),
    ],
    ids=["in-memory", "to-path", "stream"],
)
def test_download_with_missing_middle_chunk_raises_not_found(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
    download: Any,
) -> None:
    client, bot = client_with_bot

    async def scenario() -> None:
        file_id = await client.upload(file=b"abcdefghij")
        next_id = bot.messages[file_id][1]
        assert next_id is not None
        del bot.messages[int(next_id)]
        await download(client, file_id, tmp_path / "out.bin")

    with pytest.raises(discord.NotFound):
        run(scenario())


async def upload_with_broken_chain(client: Client, bot: FakeBot) -> int:
    file_id = await client.upload(file=b"abcdefghij")
    next_id = bot.messages[file_id][1]
    assert next_id is not None
    del bot.messages[int(next_id)]
    return file_id


def test_download_failure_keeps_existing_destination_untouched(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
) -> None:
    client, bot = client_with_bot
    destination = tmp_path / "out.bin"
    destination.write_bytes(b"previous")

    async def scenario() -> None:
        file_id = await upload_with_broken_chain(client, bot)
        await client.download(file_id=file_id, destination_path=destination)

    with pytest.raises(discord.NotFound):
        run(scenario())

    assert list(tmp_path.iterdir()) == [destination]
    assert destination.read_bytes() == b"previous"


def test_download_failure_leaves_nothing_in_destination_directory(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
) -> None:
    client, bot = client_with_bot

    async def scenario() -> None:
        file_id = await upload_with_broken_chain(client, bot)
        await client.download(file_id=file_id, destination_path=tmp_path / "out.bin")

    with pytest.raises(discord.NotFound):
        run(scenario())

    assert list(tmp_path.iterdir()) == []


def test_download_success_leaves_only_destination_in_directory(
    client_with_bot: tuple[Client, FakeBot],
    small_chunks: int,
    tmp_path: pathlib.Path,
) -> None:
    client, _ = client_with_bot
    destination = tmp_path / "out.bin"

    async def scenario() -> None:
        file_id = await client.upload(file=b"abcdefghij")
        await client.download(file_id=file_id, destination_path=destination)

    run(scenario())

    assert list(tmp_path.iterdir()) == [destination]
    assert destination.read_bytes() == b"abcdefghij"


def test_download_into_missing_directory_raises_file_not_found(
    client_with_bot: tuple[Client, FakeBot], tmp_path: pathlib.Path
) -> None:
    client, _ = client_with_bot

    async def scenario() -> None:
        file_id = await client.upload(file=b"abc")
        destination = tmp_path / "missing" / "out.bin"
        await client.download(file_id=file_id, destination_path=destination)

    with pytest.raises(FileNotFoundError):
        run(scenario())
    assert list(tmp_path.iterdir()) == []
