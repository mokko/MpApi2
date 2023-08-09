import aiohttp
import asyncio
from MpApi.aio.session import Session
from mpapi.constants import get_credentials
import pytest

user, pw, baseURL = get_credentials()


@pytest.mark.asyncio
async def test_context_manager():
    async with Session(user=user, pw=pw) as session:
        print(f"GH {session=}")
        assert session
