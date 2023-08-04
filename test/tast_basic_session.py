import aiohttp
import asyncio
from MpApi.aio.session import Session
import MpApi.aio.client as client
from mpapi.constants import get_credentials
import yarl

user, pw, baseURL = get_credentials()


def test_one():
    """
    Let's try the simplest version we can connect to our server using session
    in aiohttp.
    """
    print(f"Logging in as {user=}")
    print(f"{baseURL=}")

    async def main():
        acceptLang: str = "de"
        headers = {
            "Content-Type": "application/xml",
            "Accept": "application/xml;charset=UTF-8",
            "Accept-Language": acceptLang,
        }
        auth = aiohttp.BasicAuth(user, password=pw)
        async with aiohttp.ClientSession(
            auth=auth,
            # base_url=yarl.URL(baseURL),
            headers=headers,
            raise_for_status=False,
        ) as session:
            appURL = "/ria-ws/application"
            URL = yarl.URL(baseURL) / "ria-ws/application/module/definition"  # works
            # URL = "/ria-ws/application/module/definition" # ?
            print(f"URL: {URL}")
            async with session.get(URL) as response:
                print("Status:", response.status)
                print("Content-type:", response.headers["content-type"])
                text = await response.text()
                print("Body:", text[:155], "...")  #
                print(response.headers)

    asyncio.run(main())


def test_two():
    async def main():
        session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(user, password=pw),
            # base_url=yarl.URL(baseURL),
            headers={
                "Content-Type": "application/xml",
                "Accept": "application/xml;charset=UTF-8",
                "Accept-Language": "de",
            },
            raise_for_status=False,
        )
        URL = yarl.URL(baseURL) / "ria-ws/application/module/definition"  # works
        print(f"URL: {URL}")
        async with session.get(URL) as response:
            print("Status:", response.status)
            print("Content-type:", response.headers["content-type"])
            text = await response.text()
            print("Body:", text[:155], "...")  #
            print(response.headers)
        await session.close()

    asyncio.run(main())
