import asyncio
import datetime
from mpapi.constants import get_credentials
from mpapi.module import Module
from mpapi.search import Search
from MpApi.aio.chunky import Chunky
from MpApi.aio.session import Session
from pathlib import Path
import pytest

user, pw, baseURL = get_credentials()


@pytest.mark.asyncio
async def test_results():
    async with Session(user=user, pw=pw, baseURL=baseURL) as session:
        # print (f"{session=}")
        # print (f"{session.appURL}")
        chnkr = Chunky()
        rno, cno = await chnkr.count_results(
            session=session, qtype="group", target="Object", ID=182397
        )
        assert rno == 49
        assert cno == 1


@pytest.mark.asyncio
async def test_related():
    chnkr = Chunky()
    m = Module(file="getItem-Object590013.xml")
    results_set = await chnkr.analyze_related(data=m)
    assert results_set == {
        "Conservation",
        "Address",
        "Person",
        "Multimedia",
        "Literature",
        "Registrar",
        "CollectionActivity",
        "ObjectGroup",
    }


@pytest.mark.asyncio
async def test_query_maker():
    chnkr = Chunky()
    q = await chnkr.query_maker(
        qtype="group", target="Object", ID=12345, offset=100, limit=chnkr.chunk_size
    )
    assert isinstance(q, Search)


@pytest.mark.asyncio
async def test_get_by_type():
    chnkr = Chunky()
    async with Session(user=user, pw=pw, baseURL=baseURL) as session:
        m = await chnkr.get_by_type(session=session, qtype="group", ID=182397)
        assert m
        print(m)


def test_chunk_path():
    chnkr = Chunky()
    path = chnkr._chunk_path(qtype="group", ID=182397, cno=1, suffix=".xml", job="test")
    print("{path=}")
    date: str = datetime.datetime.today().strftime("%Y%m%d")

    p = Path("test") / date / "group-182397-chunk1.xml"
    assert str(path) == str(p)
