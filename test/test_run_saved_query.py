import aiohttp
import asyncio
from MpApi.aio.session import Session
from MpApi.aio.client import Client
from mpapi.constants import get_credentials
import pytest

user, pw, baseURL = get_credentials()
client = Client(baseURL=baseURL)


@pytest.mark.asyncio
async def test_run_saved_query():
    xml = """
        <application 
            xmlns="http://www.zetcom.com/ria/ws/module/search" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.zetcom.com/ria/ws/module/search 
            http://www.zetcom.com/ria/ws/module/search/search_1_6.xsd">
            <modules>
              <module name="Object">
                <search limit="10" offset="10" />
              </module>
            </modules>
        </application>
    """

    async with Session(user=user, pw=pw, timeout=100) as session:
        txt = await client.run_saved_query(session, ID=485072, mtype="Object", xml=xml)
    assert len(txt) > 500


@pytest.mark.asyncio
async def test_run_saved_query2():
    async with Session(user=user, pw=pw) as session:
        m = await client.run_saved_query2(
            session, ID=485072, mtype="Object", limit=12, offset=0
        )
    assert m
