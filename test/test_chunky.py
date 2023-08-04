import asyncio
from mpapi.constants import get_credentials
from mpapi.module import Module
from mpapi.search import Search
from MpApi.aio.chunky import Chunky
from MpApi.aio.session import Session
import pytest
user, pw, baseURL = get_credentials()

@pytest.mark.asyncio
async def test_results():
    async with Session(user=user, pw=pw, baseURL=baseURL) as session:
        #print (f"{session=}")
        #print (f"{session.appURL}")
        chnkr = Chunky()
        results = await chnkr.count_results(session=session, qtype="group", target="Object", ID=182397)
        assert results == 49 
        
@pytest.mark.asyncio
async def test_related():
    chnkr = Chunky()
    m = Module(file="getItem-Object590013.xml")
    results_set = await chnkr.analyze_related(data=m)
    assert results_set == {'Conservation', 'Address', 'Person', 'Multimedia', 'Literature', 'Registrar', 'CollectionActivity', 'ObjectGroup'}

@pytest.mark.asyncio
async def test_query_maker():
    chnkr = Chunky()
    q = await chnkr.query_maker(qtype="group", target="Object", ID=12345, offset=100, limit=chnkr.chunkSize)
    assert isinstance(q, Search)

    