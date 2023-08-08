"""
This is an experimental version of session that 
- actually returns the ClientSession and 
- doesn't contain baseURL
- provides only context manager

from MpApi.aio.session2 import Session
    with Session(user=user, pw=pw) as session:
        print (session)

"""

import aiohttp
import asyncio
from types import TracebackType
from typing import Any, Optional, Self, Type
from yarl import URL


class Session:
    """
    Open a aiohttp session with our headers providing a context manager for
    clean syntax.
    aiohttp doesn't allow a path part in base_url, so we provide our own
    see https://github.com/aio-libs/aiohttp/issues/6647
    """

    def __init__(
        self,
        *,
        user: str,
        pw: str,
        connection_limit: int = 100,
        timeout: float | None = None,
    ) -> None:
        """
        * user
        * pw
        * connection_limit represents "total number simultaneous connections. If limit is
          None the connector has no limit (default: 100)" (aiohttp.ClientSession.limit)
        * timeout controls "total number of seconds for the whole request"
          (aiohttp.ClientTimeout.total), None by default
        """
        self.user = user
        self.pw = pw
        self.connection_limit = int(connection_limit)
        self.timeout = timeout

    async def __aenter__(self) -> Self:  # params from init? ,
        """
        We could expose parameters like Accept-Language.
        """
        print(
            f"setting aiohttp timeout to {self.timeout} and max connections to {self.connection_limit}"
        )
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        conn = aiohttp.TCPConnector(limit=self.connection_limit)
        session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(self.user, password=self.pw),
            connector=conn,
            # accepts only absolute base_urls without path part
            # base_url=self.appURL
            headers={
                "Content-Type": "application/xml",
                "Accept": "application/xml;charset=UTF-8",
                "Accept-Language": "de",
            },
            raise_for_status=True,
            timeout=timeout,
        )
        self.session = session
        return session

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        await self.session.close()


if __name__ == "__main__":
    pass
