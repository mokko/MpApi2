import aiohttp
import asyncio
from mpapi.search import Search

# from mpapi.module import Module
from MpApi.aio.session2 import Session
from MpApi.aio.client2 import Client2
from mpapi.constants import get_credentials
import pytest
from yarl import URL

user, pw, baseURL = get_credentials()
client = Client2(baseURL=baseURL)

#
# let's try out limitting the session
#


@pytest.mark.asyncio
async def test_no_limit():
    """
    Get a definition from M+ with and without a specific module.
    """
    async with Session(user=user, pw=pw) as session:
        txt = await client.get_definition(session)
        assert len(txt) > 500


@pytest.mark.asyncio
async def test_determine_breaking_limit():
    async with Session(user=user, pw=pw) as session:
        coros = list()
        for cnt in range(1, 103):
            # coro = client.get_definition(session, mtype="Multimedia")
            # assert coro
            coros.append(client.get_definition(session, mtype="Multimedia"))
            print(f"connection created {cnt}")
            # assert txt
        results = await asyncio.gather(*coros)
        # breaks with 503 Service Unavailable at 200 items, succeeds with 103
        # print (results)
