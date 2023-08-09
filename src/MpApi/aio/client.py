"""
MpApi.Aio - Unofficial Asynchronous Open Source Client for MuseumPlus MpRIA 


    from MpApi.aio.session import Session
    from MpApi.aio.client import Client
    from mpapi.module import Module

    c = Client(baseURL=baseURL)

    async with Session(user=user, pw=pw) as session:
        xml = await c.getDefinition(session, mtype="Object")
        m = await c.getDefinition2(session, mtype="Object")

        xml = await c.run_saved_query(session, mtype=mtype, ID=ID, xml=query_xml)
        m = await c.run_saved_query2(session, mtype=mtype, ID=ID, limit=l, offset=o)

        txt = await c.search(session, xml=xml)
        m = await c.search2(session,query)

SEE ALSO
    https://github.com/mokko/MpApi
    http://docs.zetcom.com/ws
"""

import asyncio
import aiohttp
from aiohttp import ClientSession
import logging
import sys
from lxml import etree  # type: ignore
from mpapi.search import Search
from mpapi.module import Module
from MpApi.aio.session import Session
from types import TracebackType
from typing import Any, Optional, Type, Union
from yarl import URL
from pathlib import Path

# ET: Any
ETparser = etree.XMLParser(remove_blank_text=True)

"""
aiohttp seems to accept baseURL only a host. Path parts after the host get ignored or overwritten
"""

DEBUG = True


class Counter:
    def __init__(self):
        self._count = 0

    async def __aenter__(self) -> None:  # params from init? ,
        self._count += 1
        if DEBUG:
            print(f"DEBUG-client: {self._count} connections")

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        self._count -= 1
        if DEBUG:
            print(f"DEBUG-client: {self._count} connections")


class Client:
    def __init__(self, *, baseURL: str) -> None:
        self.baseURL = baseURL
        self.appURL = URL(baseURL) / "ria-ws/application"
        self.count = Counter()

    async def get_definition(self, session: ClientSession, *, mtype: str = None) -> str:
        if mtype is None:
            url = self.appURL / "module/definition"
        else:
            url = self.appURL / f"module/{mtype}/definition"

        async with self.count:
            response = await session.get(url)
            # Seemingly, I have to use await here otherwise I get errors.
            # I dont understand why I cant return a coro here and await it later.
            # Perhaps, because the session is closed then.
            txt = await response.text()
        return txt

    async def get_definition2(
        self, session: ClientSession, *, mtype: str = None
    ) -> Module:
        txt = await self.get_definition(session, mtype=mtype)
        return Module(xml=txt)

    async def run_saved_query(
        self, session: ClientSession, *, ID: int, mtype: str, xml: str
    ) -> str:
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
        url = self.appURL / f"module/{mtype}/search/savedQuery/{ID}"

        async with self.count:
            response = await session.post(url, data=xml)
            txt = await response.text()
        return txt

    async def run_saved_query2(
        self,
        session: ClientSession,
        *,
        ID: int,
        mtype: str,
        limit: int = -1,
        offset: int = 0,
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
        txt = await self.run_saved_query(session, ID=ID, mtype=mtype, xml=xml)
        return Module(xml=txt)

    async def search(self, session: ClientSession, *, xml: str) -> str:
        ET = etree.fromstring(bytes(xml, "UTF-8"))
        mtype = ET.xpath(
            "/s:application/s:modules/s:module/@name",
            namespaces={"s": "http://www.zetcom.com/ria/ws/module/search"},
        )[0]
        if not mtype:
            raise TypeError("Unknown module")

        url = self.appURL / f"module/{mtype}/search"

        # print(f"{mtype=} {url=}")

        async with self.count:
            response = await session.post(url, data=xml)
            txt = await response.text()
            # print(f"{response.request_info=}")
        return txt

    async def search2(self, session: ClientSession, *, query: Search) -> Module:
        query.validate(mode="search")
        txt = await self.search(session, xml=query.toString())
        return Module(xml=txt.encode())


if __name__ == "__main__":
    pass
