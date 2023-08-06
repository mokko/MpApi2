"""
MpApi.Aio - Unofficial Asynchronous Open Source Client for MuseumPlus MpRIA 

Do we need a functional style to run it asynchronously?

    async with 
    session = await create_session(user=user, password=pw)
        
    await session.close()
    await getDefinition(session, module: str = None)


OO-style

    m = client.getItem(module="Object", id="12345") # just one record? Do we really need this?



SEE ALSO
    http://docs.zetcom.com/ws
"""

import asyncio
import aiohttp
import logging
import sys
from lxml import etree  # type: ignore
from mpapi.search import Search
from mpapi.module import Module
from MpApi.aio.session import Session
from typing import Any, Union
from yarl import URL

# ET: Any
ETparser = etree.XMLParser(remove_blank_text=True)

"""
aiohttp seems to accept baseURL only a host. Path parts after the host get ignored or overwritten
"""


async def get_definition(session: Session, *, mtype: str = None) -> str:
    if mtype is None:
        url = session.appURL / "module/definition"
    else:
        url = session.appURL / f"module/{mtype}/definition"
    async with session.session.get(url) as response:
        return await response.text()
        # this way doesn't match the synchronous client
        # or do we want to return a coro?
        # return response doesn't seem to work


async def get_definition2(session: Session, *, mtype: str = None) -> Module:
    txt = await get_definition(session, mtype=mtype)
    return Module(xml=txt)


async def run_saved_query(session: Session, *, ID: int, mtype: str, xml: str) -> str:
    """
    Run a pre-existing saved search
    POST http://.../ria-ws/application/module/{module}/search/savedQuery/{__id}

    Zetcom reminds us: A request body must be provided, in order to control the paging.
    For example:

    <application
        xmlns="http://www.zetcom.com/ria/ws/module/search"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.zetcom.com/ria/ws/module/search http://www.zetcom.com/ria/ws/module/search/search_1_4.xsd">
        <modules>
          <module name="Object">
            <search limit="10" offset="0" />
          </module>
        </modules>
    </application>
    """
    url = session.appURL / f"module/{mtype}/search/savedQuery/{ID}"
    async with session.session.post(url, data=xml) as response:
        return await response.text()


async def run_saved_query2(
    session: Session, *, ID: int, mtype: str, limit: int = -1, offset: int = 0
) -> Module:
    """
    Like run_saved_query just with
    - limit and offset as parameters
    - query validation
    - returns results in Module
    """

    xml = f"""
            <application 
                xmlns="http://www.zetcom.com/ria/ws/module/search" 
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                xsi:schemaLocation="http://www.zetcom.com/ria/ws/module/search 
                http://www.zetcom.com/ria/ws/module/search/search_1_6.xsd">
                <modules>
                  <module name="{mtype}">
                    <search limit="{limit}" offset="{offset}" />
                  </module>
                </modules>
            </application>
        """
    q = Search(fromString=xml)
    q.validate(mode="search")
    txt = await run_saved_query(session, ID=ID, mtype=mtype, xml=xml)
    return Module(xml=txt)


async def search(session, *, xml: str) -> str:
    ET = etree.fromstring(bytes(xml, "UTF-8"))
    mtype = ET.xpath(
        "/s:application/s:modules/s:module/@name",
        namespaces={"s": "http://www.zetcom.com/ria/ws/module/search"},
    )[0]
    if not mtype:
        raise TypeError("Unknown module")

    url = session.appURL / f"module/{mtype}/search"

    # print(f"{mtype=} {url=}")

    async with session.session.post(url, data=xml) as response:
        # print(f"{response.request_info=}")
        return await response.text()


async def search2(session, *, query: Search) -> Module:
    query.validate(mode="search")
    txt = await search(session, xml=query.toString())
    return Module(xml=txt.encode())


if __name__ == "__main__":
    from mpapi.constants import get_credentials

    user, pw, baseURL = get_credentials()
    # session = MpApi.Aio._create_session(user=user, pw=pw, baseURL=baseURL)
