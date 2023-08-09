import aiohttp
import asyncio
from MpApi.aio.session import Session
from mpapi.constants import get_credentials

user, pw, baseURL = get_credentials()


def test_context_manager():
    async def main():
        async with Session(user=user, pw=pw) as session:
            print(f"GH {session=}")
            assert session

    asyncio.run(main())
