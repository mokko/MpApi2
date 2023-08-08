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
c = Client2(baseURL=baseURL)


@pytest.mark.asyncio
async def test_definition_no_module():
    """
    Get a definition from M+ with and without a specific module.
    """
    async with Session(user=user, pw=pw) as session:
        txt = await c.get_definition(session)
        # print(txt)
        assert len(txt) > 500


@pytest.mark.asyncio
async def test_definition_with_module():
    async with Session(user=user, pw=pw) as session:
        txt = await c.get_definition(session, mtype="Multimedia")
        # print(txt)
        assert len(txt) > 500


@pytest.mark.asyncio
async def test_get_definition2_no_module():
    """
    same as above just use get_definition2 instead of get_definition, where
    we get a Module object back.
    """
    async with Session(user=user, pw=pw) as session:
        m = await c.get_definition2(session)
        print(m)
        assert m


@pytest.mark.asyncio
async def test_get_definition2_with_module():
    async with Session(user=user, pw=pw) as session:
        m = await c.get_definition2(session, mtype="Multimedia")
        print(m)
        assert m


# test search


@pytest.mark.asyncio
async def test_search():
    q = Search(module="Object")
    q.addCriterion(
        field="ObjObjectGroupsRef.__id",
        operator="equalsField",
        value=str(182397),
    )
    q.addField(field="__id")
    q.validate(mode="search")

    async with Session(user=user, pw=pw) as session:
        txt = await c.search(session, xml=q.toString())
    # print(txt)
    assert txt


@pytest.mark.asyncio
async def test_search2():
    q = Search(module="Object")
    q.addCriterion(
        field="ObjObjectGroupsRef.__id",
        operator="equalsField",
        value=str(182397),
    )
    q.addField(field="__id")
    q.validate(mode="search")

    async with Session(user=user, pw=pw) as session:
        m = await c.search2(session, query=q)

    m.toFile(path="debug.search-response.xml")
    assert m


@pytest.mark.asyncio
async def tast_search_single():
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

    async with Session(user=user, pw=pw) as session:
        m = await c.search2(session, query=q)

    totalSize = m.totalSize(module="Object")
    print(f"{totalSize=}")
    m.toFile(path="debug.search-response.xml")
    assert m
