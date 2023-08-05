"""
Async version of the chunker. Uses deterministic search.

chunking logarithm
    for a given query, 
    (1) determine number of results and  number of chunks to get all results
    (2) d/l first chunk, 
    (3) get all related items and assemble everything in one chunk
    (4) zip and save the chunk
    (5) contiue at (3) for the next chunk until last chunk

USAGE
    from MpApi.aio.chunky import Chunky
    from MpApi.aio.session import Session
    
    chnkr = Chunky()
    async with Session(user=user,pw=pw, baseURL=baseURL):
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
allowed_mtypes = ["Multimedia", "Object", "Person"]  # todo: teach me more
# ObjectGroup
import asyncio
import MpApi.aio.client as client
from MpApi.aio.session import Session
from mpapi.constants import NSMAP
from mpapi.module import Module
from mpapi.search import Search
from typing import Iterator


class Chunky:
    def __init__(
        self,
        *,
        chunk_size: int = 1000,
    ) -> None:
        self.chunk_size = int(chunk_size)

    async def analyze_related(self, *, data: Module) -> set:
        """
        Return a set of targetModules in the provided data.
        """
        targetL = data.xpath(
            "/m:application/m:modules/m:module/m:moduleItem/m:moduleReference/@targetModule"
        )
        return {target for target in targetL}

    async def count_results(
        self, *, session: Session, qtype: str, target: str, ID: int
    ) -> int:
        """
        Return the number of results for a given query. The query is described by query
        type (qtype, e.g. group), ID, and target (target): e.g. group 1234 Object. Takes
        one fast http query.
        """
        q = await self.query_maker(qtype=qtype, target=target, ID=ID, offset=0, limit=1)
        print(f"{str(q)}")
        q.addField(field="__id")
        q.validate(mode="search")
        m = await client.search2(session, query=q)
        rno = m.totalSize(module=target)
        chnk_no = int(rno / self.chunk_size) + 1  # no of chunks
        return rno, chnk_no

    async def get_by_type(
        self,
        *,
        ID: int,
        qtype: str,
        session: Session,
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

        q = Search(module="Object", limit=self.chunk_size, offset=0)

        q.addCriterion(
            field=fields[qtype],
            operator="equalsField",
            value=str(ID),
        )
        q.validate(mode="search")
        return await client.search2(session, query=q)

    async def get_related_items(self, *, session: Session, data: Module, target: str):
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
        q.validate(mode="search")
        relatedM = await client.search2(session, query=q)
        return relatedM

    async def query_maker(
        self, *, ID, qtype: str, target: str, offset: int = 0, limit: int = -1
    ):
        """
        Let's have a factory that makes all Search objects we need.

        query type: approval, exhibit, group, loc
        mtype/module type: Object, Person, Multimedia ...

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
            raise ValueError("Query type not allowed: {qtype=}")

        if target not in allowed_mtypes:
            raise ValueError("Module type not allowed: {target=}")

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
