import aiohttp
import asyncio
from MpApi.aio.session import Session
import MpApi.aio.client as client
from mpapi.constants import get_credentials
import yarl

user, pw, baseURL = get_credentials()


def test_get_definition():
    print(f"{baseURL=}")

    async def no_module():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            txt = await client.get_definition(session)
            # print(txt)
            assert len(txt) > 500

    asyncio.run(no_module())

    async def with_module():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            txt = await client.get_definition(session, mtype="Multimedia")
            # print(txt)
            assert len(txt) > 500

    asyncio.run(with_module())


def test_get_definition2():
    async def no_module():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            m = await client.get_definition2(session)
            # print(m)
            assert m

    asyncio.run(no_module())

    async def with_module():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            m = await client.get_definition2(session, mtype="Multimedia")
            # print(m)
            assert m

    asyncio.run(with_module())
