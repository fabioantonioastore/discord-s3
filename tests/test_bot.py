import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from discord_s3._impl import bot as bot_module
from discord_s3._impl.bot import Bot


def run_with_bot(scenario: Callable[[Bot], Awaitable[Any]]) -> Any:
    async def main() -> Any:
        bot = Bot(intents=discord.Intents.none())
        try:
            return await scenario(bot)
        finally:
            await bot.async_close()

    return asyncio.run(main())


def text_channel(name: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        type=discord.ChannelType.text,
        send=AsyncMock(return_value=SimpleNamespace(id=1)),
        fetch_message=AsyncMock(return_value=SimpleNamespace(id=5)),
    )


def fake_guild(*channels: SimpleNamespace) -> MagicMock:
    guild = MagicMock()
    guild.channels = list(channels)
    guild.create_text_channel = AsyncMock(return_value=text_channel("storage"))
    return guild


def exhaust_requests(bot: Bot, monkeypatch: pytest.MonkeyPatch, limit: int) -> None:
    monkeypatch.setattr(bot_module, "MAX_GLOBAL_REQUESTS", limit)
    for _ in range(limit):
        bot._Bot__increment_global_requests()  # type: ignore[attr-defined]


async def init_with_guild(bot: Bot, guild: MagicMock | None) -> MagicMock:
    get_guild = MagicMock(return_value=guild)
    bot.get_guild = get_guild  # type: ignore[method-assign]
    await bot.init(guild_id=99)
    return get_guild


def test_new_bot_is_able_to_make_request() -> None:
    async def scenario(bot: Bot) -> bool:
        return bot.is_able_to_make_request()

    assert run_with_bot(scenario) is True


def test_bot_is_not_able_to_make_request_after_reaching_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario(bot: Bot) -> bool:
        exhaust_requests(bot, monkeypatch, limit=2)
        return bot.is_able_to_make_request()

    assert run_with_bot(scenario) is False


def test_request_counter_resets_after_reset_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot_module, "GLOBAL_REQUESTS_RESET_TIME", 0.01)

    async def scenario(bot: Bot) -> tuple[bool, bool]:
        await asyncio.sleep(0)
        exhaust_requests(bot, monkeypatch, limit=2)
        before = bot.is_able_to_make_request()
        await asyncio.sleep(0.05)
        return before, bot.is_able_to_make_request()

    assert run_with_bot(scenario) == (False, True)


def test_async_close_cancels_reset_task_and_closes_client() -> None:
    async def main() -> tuple[bool, bool]:
        bot = Bot(intents=discord.Intents.none())
        await bot.async_close()
        await asyncio.sleep(0)
        task = bot._Bot__global_requests_reset_task  # type: ignore[attr-defined]
        return task.cancelled(), bot.is_closed()

    assert asyncio.run(main()) == (True, True)


def test_init_raises_when_guild_is_not_found() -> None:
    async def scenario(bot: Bot) -> None:
        await init_with_guild(bot, None)

    with pytest.raises(RuntimeError, match="Guild not found"):
        run_with_bot(scenario)


def test_init_reuses_existing_storage_text_channel() -> None:
    storage = text_channel("storage")
    voice_storage = SimpleNamespace(name="storage", type=discord.ChannelType.voice)
    guild = fake_guild(text_channel("general"), voice_storage, storage)

    async def scenario(bot: Bot) -> MagicMock:
        get_guild = await init_with_guild(bot, guild)
        await bot.upload(file=b"data", content=None)
        return get_guild

    get_guild = run_with_bot(scenario)

    get_guild.assert_called_once_with(99)
    guild.create_text_channel.assert_not_awaited()
    storage.send.assert_awaited_once()


def test_init_creates_storage_channel_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot_module, "MAX_GLOBAL_REQUESTS", 1)
    guild = fake_guild(text_channel("general"))

    async def scenario(bot: Bot) -> bool:
        await init_with_guild(bot, guild)
        return bot.is_able_to_make_request()

    able_after_init = run_with_bot(scenario)

    guild.create_text_channel.assert_awaited_once_with(name="storage")
    assert able_after_init is False


def test_upload_sends_file_with_content_to_storage_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot_module, "MAX_GLOBAL_REQUESTS", 1)
    storage = text_channel("storage")
    guild = fake_guild(storage)

    async def scenario(bot: Bot) -> tuple[Any, bool]:
        await init_with_guild(bot, guild)
        message = await bot.upload(file=b"payload", content="42")
        return message, bot.is_able_to_make_request()

    message, able_after_upload = run_with_bot(scenario)

    assert message is storage.send.return_value
    kwargs = storage.send.await_args.kwargs
    assert kwargs["content"] == "42"
    assert isinstance(kwargs["file"], discord.File)
    assert kwargs["file"].fp.read() == b"payload"
    assert able_after_upload is False


def test_get_message_fetches_message_by_id_from_storage_channel() -> None:
    storage = text_channel("storage")
    guild = fake_guild(storage)

    async def scenario(bot: Bot) -> Any:
        await init_with_guild(bot, guild)
        return await bot.get_message(message_id=5)

    message = run_with_bot(scenario)

    storage.fetch_message.assert_awaited_once_with(5)
    assert message is storage.fetch_message.return_value


def test_get_message_counts_as_a_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bot_module, "MAX_GLOBAL_REQUESTS", 1)
    guild = fake_guild(text_channel("storage"))

    async def scenario(bot: Bot) -> bool:
        await init_with_guild(bot, guild)
        await bot.get_message(message_id=5)
        return bot.is_able_to_make_request()

    assert run_with_bot(scenario) is False


def test_get_message_propagates_not_found_for_missing_message() -> None:
    storage = text_channel("storage")
    response = MagicMock(status=404, reason="Not Found")
    storage.fetch_message = AsyncMock(
        side_effect=discord.NotFound(response, "Unknown Message")
    )
    guild = fake_guild(storage)

    async def scenario(bot: Bot) -> None:
        await init_with_guild(bot, guild)
        await bot.get_message(message_id=404)

    with pytest.raises(discord.NotFound):
        run_with_bot(scenario)
