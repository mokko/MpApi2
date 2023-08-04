import aiohttp
import asyncio
from MpApi.aio.session import Session
import MpApi.aio.client as client
from mpapi.constants import get_credentials
import yarl

user, pw, baseURL = get_credentials()

"""
2023-07 30 tests 
get_definition, search
"""


def test_get_definition():
    """
    Get a definition from M+ with and without a specific module.
    """
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
    """
    same as above just use get_definition2 instead of get_definition, where
    we get a Module object back.
    """
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

#
# test search
#
def test_search():
    q = Search(module="Object")
    q.addCriterion(
        field="ObjObjectGroupsRef.__id",
        operator="equalsField",
        value=str(182397),
    )
    q.addField(field="__id")
    q.validate(mode="search")

    async def main():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            txt = await client.search(session, xml=q.toString())
            print(txt)
            assert txt

    asyncio.run(main())


def test_search2():
    q = Search(module="Object")
    q.addCriterion(
        field="ObjObjectGroupsRef.__id",
        operator="equalsField",
        value=str(182397),
    )
    q.addField(field="__id")
    q.validate(mode="search")

    async def main():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            m = await client.search2(session, query=q)
            return m

    m = asyncio.run(main())
    # m.toFile(path="debug.search-response.xml")
    assert m


def tast_search_single():
    """
    We query the objects from one group which has about 49 objects, but we limit query 
    to 1. Response still reports the total number of hits, in this case 49 in totalSize
    although it only includes a single item.
    
    In this version of the test, we write response to file.
    """
    q = Search(module="Object", limit=1, offset=0)
    q.addCriterion(
        field="ObjObjectGroupsRef.__id",
        operator="equalsField",
        value=str(182397),
    )
    q.addField(field="__id")
    q.validate(mode="search")

    async def main():
        async with Session(user=user, pw=pw, baseURL=baseURL) as session:
            m = await client.search2(session, query=q)
            return m

    m = asyncio.run(main())
    totalSize = m.totalSize(module="Object")
    print(f"{totalSize=}")
    m.toFile(path="debug.search-response.xml")
    assert m
