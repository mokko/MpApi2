"""
from MpApi.aio.session import Session
    with Session(user=user, pw=pw) as session:
        print (session)

    s = Session(user=user,pw=pw, baseURL=baseURL)
    session = s.get_session()
"""

import aiohttp
import asyncio
from types import TracebackType
from typing import Any, Optional, Self, Type
from yarl import URL

MAX_CONNECTION = 100  # surprisingly, seems to result in aiohttp.client_exceptions.ServerTimeoutError: Connection
TIMEOUT = float(600)  # seconds


class Session:
    """
    Open a aiohttp session with our headers providing a context manager for
    clean syntax.
    aiohttp doesn't allow a path part in base_url, so we provide our own
    see https://github.com/aio-libs/aiohttp/issues/6647
    """

    def __init__(self, *, user: str, pw: str, baseURL: str, timeout: int = 300) -> None:
        self.user = user
        self.pw = pw
        self.baseURL = baseURL
        self.appURL = URL(baseURL) / "ria-ws/application"

    async def close(self) -> None:
        """Tested by now?"""
        try:
            self.session
        except AttributeError:
            pass
        else:
            await self.session.close()
            del self.session

    async def get(self) -> Any:
        """
        We want access to the session. This coroutine provides two ways to get to the session.
        (a) it makes sure it's available at self.session
        (b) it also returns it directly.

        Use close() to close the session.
        """
        try:
            self.session
        except:
            await self.__aenter__()
        finally:
            return self.session

    async def __aenter__(self) -> Self:  # params from init? ,
        """
        We could expose parameters like Accept-Language.
        """
        print(
            f"setting aiohttp timeout to {TIMEOUT} and max connections to {MAX_CONNECTION}"
        )
        timeout = aiohttp.ClientTimeout(total=TIMEOUT, connect=float(10))
        # timeout = aiohttp.ClientTimeout()
        conn = aiohttp.TCPConnector(limit=MAX_CONNECTION)
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
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        await self.close()


if __name__ == "__main__":
    from mpapi.constants import get_credentials
    from MpApi.aio.session import Session

    user, pw, baseURL = get_credentials()

    async def main() -> None:
        async with Session(user=user, pw=pw) as session:
            print(session)

    asyncio.run(main())
