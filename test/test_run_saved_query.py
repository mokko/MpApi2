import aiohttp
import asyncio
from MpApi.aio.session import Session
import MpApi.aio.client as client
from mpapi.constants import get_credentials

user, pw, baseURL = get_credentials()


def test_run_saved_query():
    xml = """
        <application 
            xmlns="http://www.zetcom.com/ria/ws/module/search" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.zetcom.com/ria/ws/module/search 
            http://www.zetcom.com/ria/ws/module/search/search_1_6.xsd">
            <modules>
              <module name="Object">
                <search limit="-1" offset="10" />
              </module>
            </modules>
        </application>
    """

    async def saved_query():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            txt = await client.run_saved_query(
                session, ID=485072, mtype="Object", xml=xml
            )

    assert len(txt) > 500
    asyncio.run(saved_query())


def test_run_saved_query2():
    async def saved_query():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            m = await client.run_saved_query2(
                session, ID=485072, mtype="Object", limit=12, offset=0
            )

    assert m
    asyncio.run(saved_query())
