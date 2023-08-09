"""
Async version of the chunker. Uses deterministic search.

chunking logarithm
    for a given query, 
    (1) determine number of results and  number of chunks to get all results
    (2) d/l first chunk objects only, 
    (3) get all related items and assemble everything in one chunk
    (4) zip and save the chunk
    (5) contiue at (3) for the next chunk until last chunk

USAGE
    from MpApi.aio.chunky import Chunky
    from MpApi.aio.session import Session
    
    chnkr = Chunky(baseURL=baseURL)
    async with Session(user=user,pw=pw):
        for chunk in chnkr.get_by_type(qtype=qtype, ID=ID):
            chunk.toZip(path="file.xml.zip")

    # other
        q = await chnkr.query_maker(qtype=qt, ID=ID, target=target)
        res_no, chk_no = await chnkr.count_results(qtype="group", target="Object", ID=1234)
        relatedTypesL = await chnkr.analyze_related(data=m)
        for target in relatedTypesL:
            data = get_related_items(data=m, target=target):
"""

allowed_query_types = ["approval", "exhibit", "group", "loc", "query"]
allowed_mtypes = ["Multimedia", "Object", "Person"]  # for query_maker

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from aiohttp import ClientSession

import asyncio
import datetime
from MpApi.aio.client import Client
from MpApi.aio.session import Session
from mpapi.constants import NSMAP
from mpapi.module import Module
from mpapi.search import Search
from typing import Iterator
from pathlib import Path

# chunk_sem_int = 3
# rel_sem_int = 7


class Chunky:
    def __init__(
        self, *, baseURL: str, chunk_size: int = 1000, exclude_modules: list = []
    ) -> None:
        self.baseURL = baseURL
        self.chunk_size = int(chunk_size)
        self.client = Client(baseURL=baseURL)
        self.exclude_modules = exclude_modules
        # print(f"asyncio timeout {TIMEOUT} sec = {int(TIMEOUT/60)} min")
        # print(f"Setting semaphores to {chunk_sem_int}/{rel_sem_int} (chunk,related)")
        # self.chunk_sem = asyncio.Semaphore(chunk_sem_int)
        # self.related_sem = asyncio.Semaphore(rel_sem_int)

    async def analyze_related(self, *, data: Module) -> set:
        """
        Return a set of targetModules in the provided data.
        """
        targetL = data.xpath(
            "/m:application/m:modules/m:module/m:moduleItem/m:moduleReference/@targetModule"
        )
        return {target for target in targetL}

    async def apack_all_chunks(
        self,
        session: ClientSession,
        *,
        ID: int,
        job: str,
        qtype: str,
    ) -> None:

        # no of results; chunks needed for results
        # target is always Object?
        rno, cmax = await self.count_results(
            session, qtype=qtype, target="Object", ID=ID
        )
        if not rno:
            print("Nothing to download!")
            return
        print(f"{rno=} {cmax=}")

        chunkL = list()
        for cno in range(1, cmax):  # cmax is 0-based
            # async with asyncio.TaskGroup() as tg:
            # async with asyncio.timeout(TIMEOUT):
            coro = self.apack_per_chunk(
                session,
                cno=cno,
                ID=ID,
                job=job,
                qtype=qtype,
            )
            chunkL.append(asyncio.create_task(coro))
            # chunkL.append(tg.create_task(coro))
        # async with self.chunk_sem:
        try:
            await asyncio.gather(*chunkL)
        except* Exception as e:
            print("... attempting graceful shutdown (chunky.py:108)")
            await session.close()
            raise e

    async def apack_per_chunk(
        self,
        session: ClientSession,
        *,
        cno: int,
        ID: int,
        job: str,
        qtype: str,
    ) -> None:
        offset = int(cno - 1) * self.chunk_size
        # 1: 0 * 1000 = 0
        # 2: 1 * 1000 = 1000
        print(f"   getting {cno}-Objects by qtype '{qtype}' /w offset {offset}...")
        # async with self.related_sem:
        chunk = await self.get_by_type(session, qtype=qtype, ID=ID, offset=offset)

        chunk_fn = self._chunk_path(qtype=qtype, ID=ID, cno=cno, job=job, suffix=".xml")
        # print(f"{chunk_fn} is the right path for this chunk")

        related_targetsL = await self.analyze_related(data=chunk)
        # related_coroL = list()
        relatedL1 = list()
        # async with asyncio.TaskGroup() as tg:
        for target in sorted(related_targetsL):
            if target in self.exclude_modules:
                print(f"   ignoring {cno}-{target}")
                continue

            print(f"   getting {cno}-{target} (related)")
            # async with asyncio.timeout(TIMEOUT):
            coro = self.get_related_items(session, data=chunk, target=target)
            relatedL1.append(asyncio.create_task(coro))
        related_resL = await asyncio.gather(*relatedL1)
        for relatedM in related_resL:
            target = relatedM.extract_mtype()
            print(f"   adding related {target} {len(relatedM)} items... ")
            chunk += relatedM
        print(f"zipping multi chunk {chunk_fn}...")
        chunk.clean()
        chunk.toZip(path=chunk_fn)  # write zip file to disk

        print("validating multi chunk...", end="")
        chunk.validate()
        print("done")

    async def count_results(
        self, session: ClientSession, *, qtype: str, target: str, ID: int
    ) -> int:
        """
        Return the number of results for a given query. The query is described by query
        type (qtype, e.g. group), ID, and target (target): e.g. group 1234 Object. Takes
        one fast http query.
        """
        q = await self.query_maker(qtype=qtype, target=target, ID=ID, offset=0, limit=1)
        # print(f"{str(q)}")
        q.addField(field="__id")
        q.validate(mode="search")
        m = await self.client.search2(session, query=q)
        rno = m.totalSize(module=target)
        chnk_no = int(rno / self.chunk_size) + 1  # no of chunks
        return rno, chnk_no

    async def get_by_type(
        self,
        session: ClientSession,
        *,
        ID: int,
        qtype: str,
        offset: int = 0,
    ) -> Module:
        """
        Gets one chunk of Objects. Limit is automatically set to chunk_size. Returns
        a Module object.
        """

        fields: dict = {  # TODO: untested
            "approval": "ObjPublicationGrp.TypeVoc",
            "exhibit": "ObjRegistrarRef.RegExhibitionRef.__id",
            "group": "ObjObjectGroupsRef.__id",
            "loc": "ObjCurrentLocationVoc",
        }

        q = Search(module="Object", limit=self.chunk_size, offset=offset)

        q.addCriterion(
            field=fields[qtype],
            operator="equalsField",
            value=str(ID),
        )
        q.validate(mode="search")
        # print(str(q))
        # async with asyncio.timeout(TIMEOUT):
        m = await self.client.search2(session, query=q)
        return m

    async def get_related_items(
        self, session: ClientSession, *, data: Module, target: str
    ):
        """
        Given some object data, query for related records. Related records are
        those linked to from inside the object data. Return a new module of target type.
        """
        dataET = data.toET()

        IDs: Any = dataET.xpath(
            f"//m:moduleReference[@targetModule = '{target}']/m:moduleReferenceItem/@moduleItemId",
            namespaces=NSMAP,
        )

        q = Search(module=target, limit=-1, offset=0)
        relIDs = set(IDs)  # IDs are not necessarily unique, but we want unique
        count = 1  # one-based out of tradition; counting unique IDs
        for ID in sorted(relIDs):
            # print(f"{target} {ID}")
            if count == 1 and len(relIDs) > 1:
                q.OR()
            q.addCriterion(
                operator="equalsField",
                field="__id",
                value=str(ID),
            )
            count += 1
        if target == "Address":
            # I wish I could exclude only the offending way to long field
            q.addField(field="__id")
            q.addField(field="__lastModifiedUser")
            q.addField(field="__lastModified")
            q.addField(field="__createdUser")
            q.addField(field="__created")
            q.addField(field="__orgUnit")
            q.addField(field="AdrSortTxt")
            q.addField(field="AdrCityTxt")
            q.addField(field="AdrNotesClb")
            q.addField(field="AdrOrganisationTxt")
            q.addField(field="AdrPostcodeTxt")
            q.addField(field="AdrStreetTxt")
            q.addField(field="AdrCatEntryTxt")
            q.addField(field="AdrCatNameTxt")
            q.addField(field="AdrCatLocationTxt")
            q.addField(field="AdrTypeVoc")
            q.addField(field="AdrContactGrp")

        q.validate(mode="search")
        q.toFile(path=f"debug.related.{target}.xml")
        # async with asyncio.timeout(TIMEOUT):

        # async with self.related_sem:
        relatedM = await self.client.search2(session, query=q)
        return relatedM

    async def query_maker(
        self, *, ID, qtype: str, target: str, offset: int = 0, limit: int = -1
    ):
        """
        Let's have a factory that makes all Search objects we need in chunky.py.

        q(uery)type: approval, exhibit, group, loc
        mtype: Object, Person, Multimedia ...

        We start with a group that corresponds with the query type: approval group,
        exhibit, [object] group, loc. It's a group of objects defined by a common trait /
        feature.

        We get the objects in that group, then all the related records in a number of
        related modules (e.g. Multimedia, Person).

        Can we assume that we always want to get objects? Not really. There may be
        scenarios where we only want Multimedia or Person. But we could for now assume
        that we always start with objects and then add whatever else has been elected.

        Get all objects in the approval group, exhibit, group, loc.
        Get all multimedia records related to the

        For multimedia do we export everything or only approved/freigegebene records?
        Since we could reduce the strain on the server, let's typically only

        In the old chunker, we did collect a lot objIds and requested related objects
        for these IDs. I dont wanna do that here because I would have to wait for the
        object IDs to arrive before I can query related items. But perhaps this still
        remains the best way to go since otherwise there is no guarantee that I get
        exactly the records related to the objects in the corresponding chunk. If that is the case

        """

        if qtype not in allowed_query_types:
            raise ValueError(f"Query type not allowed: {qtype=}")

        if target not in allowed_mtypes:
            raise ValueError(f"Module type not allowed: {target=}")

        # "Registrar": "RegExhibitionRef.__id",

        fields: dict = {
            "Multimedia": {
                "approval": "MulObjectRef.ObjPublicationGrp.TypeVoc",
                "exhibit": "MulObjectRef.ObjRegistrarRef.RegExhibitionRef.__id",
                "group": "MulObjectRef.ObjObjectGroupsRef.__id",
                "loc": "MulObjectRef.ObjCurrentLocationVoc",
            },
            "Object": {
                "approval": "ObjPublicationGrp.TypeVoc",
                "exhibit": "ObjRegistrarRef.RegExhibitionRef.__id",
                "group": "ObjObjectGroupsRef.__id",
                "loc": "ObjCurrentLocationVoc",
            },
            "Person": {
                "approval": "PerObjectRef.ObjPublicationGrp.TypeVoc",
                "exhibit": "PerObjectRef.ObjRegistrarRef.RegExhibitionRef.__id",
                "group": "PerObjectRef.ObjObjectGroupsRef.__id",
                "loc": "PerObjectRef.ObjCurrentLocationVoc",
            },
        }
        pubVoc: dict = {
            "Multimedia": "MulObjectRef.ObjPublicationGrp.PublicationVoc",
            "Object": "ObjPublicationGrp.PublicationVoc",
            "Person": "PerObjectRef.ObjPublicationGrp.PublicationVoc",
        }
        q = Search(module=target, offset=offset, limit=limit)
        if qtype == "approval" or target == "Multimedia":
            q.AND()
        q.addCriterion(
            field=fields[target][qtype],
            operator="equalsField",
            value=str(ID),
        )
        if qtype == "approval":
            # approval group needs two criteria
            q.addCriterion(
                field=str(pubVoc[target]),
                operator="equalsField",
                value="1810139",  # 1810139: yes
            )
        if target == "Multimedia":
            # CAUTION: filter for approved Multimedia records
            q.addCriterion(
                operator="equalsField",
                field="MulApprovalGrp.TypeVoc",
                value="1816002",  # SMB-Digital = 1816002
            )
            q.addCriterion(
                field="MulApprovalGrp.ApprovalVoc",
                operator="equalsField",
                value="1810139",  # todo 1810139 is not right. CHECK!
            )
        return q

    #
    #
    #

    def _chunk_path(
        self, *, qtype: str, ID: int, cno: int, job: str, suffix: str = ".xml"
    ) -> Path:
        """
        Rerurn the a path for the current chunk. Relies on self.job being set
        (which gets set in run_job).

        Note: I was thinking of moving this to chunky, but I dont have Path there and
        self.job.
        """
        if job is None:
            raise TypeError("ERROR: No job name. Can't create project dir!")
        date: str = datetime.datetime.today().strftime("%Y%m%d")
        project_dir: Path = Path(job) / date
        if not project_dir.is_dir():
            Path.mkdir(project_dir, parents=True)
        return project_dir / f"{qtype}-{ID}-chunk{cno}{suffix}"
