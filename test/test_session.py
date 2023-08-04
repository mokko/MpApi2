import aiohttp
import asyncio
from MpApi.aio.session import Session
from mpapi.constants import get_credentials

user, pw, baseURL = get_credentials()


def test_context_manager():
    async def main():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            print(f"GH {session=}")
            print(f"GH {session.session=}")
            assert session.session

    asyncio.run(main())


def test_get_session():
    async def main():
        s = Session(user=user, pw=pw, baseURL=baseURL)
        session = await s.init()
        # both ways should work
        # return session
        return s.session

    session = asyncio.run(main())  # can have a return value
    assert session
    print(f"FF {session}")
