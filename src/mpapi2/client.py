"""
MpApi - Unofficial Async Open Source Client for MuseumPlus MpRIA 

This is a low level client that doesn't do much on its own

    m = client.getItem(module="Object", id="12345") # just one record? Do we really need this?


SEE ALSO
    http://docs.zetcom.com/ws
"""

import asyncio
import aiohttp
import logging
import sys
from lxml import etree  # type: ignore
from mpapi.search import Search
from mpapi.module import Module
from typing import Any, Union

# ET: Any
ETparser = etree.XMLParser(remove_blank_text=True)

user = "EM_MM"
pw = "K@somba2!"
baseURL = "https://museumplus-produktiv.spk-berlin.de:8181/MpWeb-mpBerlinStaatlicheMuseen"


class MpApi2:

##
## privates
##           

    async def _delete(self, url, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        r = await session.delete(url=url)
        #async with self.session.delete(url=url) as r:
        #print(r.status)
        return r

    async def _get(self, url, *, headers=None, stream=False, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        if headers is None:
            headers = {}
        r = await session.get(url, headers=headers)
        return r

    async def _post(self, url, *, data, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        r = await session.post(url, data=data)
        return r

    async def _put(self, url, *, data, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        r = await session.put(url, data=data)
        return r

    async def _search(self, *, queryET, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """
        A version of the search method that expects the query as etree document
        and returns requests response.
        """

        mtype = queryET.xpath(
            "/s:application/s:modules/s:module/@name",
            namespaces={"s": "http://www.zetcom.com/ria/ws/module/search"},
        )[0]
        if not mtype:
            raise TypeError("Unknown module")
        url = f"{self.appURL}/module/{mtype}/search"

        return await self._post(
            url,
            data=etree.tostring(queryET), 
            session=session
        )

    #
    # A SESSION
    #

    async def getSessionKey(self) -> str:  # should be int?
        """
        GET http://.../ria-ws/application/session
        """
        url = self.appURL + "/session"
        r = self._get(url)
        tree = self.ETfromString(xml=r.text)

        key = tree.xpath(
            "/s:application/s:session/s:key/text()",
            namespaces={"s": "http://www.zetcom.com/ria/ws/session"},
        )
        return key

    #
    # B REQUESTS WITH module.xsd response?
    #
    # B.1 DATA async defINITIONs
    #
    async def _definition(self, *, module: str = None) -> aiohttp.ClientResponse:
        """
        Retrieve the data async definition of a single or of all modules
        GET http://.../ria-ws/application/module/{module}/async definition/
        GET http://.../ria-ws/application/module/async definition/

        returns a request containing the module async definition for specified module
        or all modules if no module is specified.
        """

        if module is None:
            url = self.appURL + "/module/async definition"
        else:
            url = self.appURL + "/module/" + module + "/async definition"

        return await self._get(url)

    async def definition(self, *, mtype: str = None) -> aiohttp.ClientResponse:
        """
        Return the data async definition for a single or all modules
        """
        return await _definition(module=mtype)

    #
    # B.2 SEARCH
    #
    async def runSavedQuery(self, *, id: int, mtype: str, xml: str) -> aiohttp.ClientResponse:
        """
        Run a pre-existing saved search
        POST http://.../ria-ws/application/module/{module}/search/savedQuery/{__id}

        N.B. / Caveats
        - Currently only queries targeting the Object module are supported! Ideally,
          client.py would extract that piece of information from the query.
        - api does not support since. If you need that include that since date in your
          RIA query.

        Quote from http://docs.zetcom.com/ws/:
        A request body must be provided, in order to control the paging. For example:

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
        url = f"{self.appURL}/module/{mtype}/search/savedQuery/{id}"
        return await self._post(url, data=xml)

    async def runSavedQuery2(
        self, *, ID: int, Type: str = "Object", limit: int = -1, offset: int = 0
    ):  # -> ET
        """
        Higher level version of runSavedQuery where you specify limit and
        offset with parameters and return result as Module object.

        Expects
        - ID: integer
        - Type: target type (module type of the results)
        - limit
        - offset
        Returns
        - result document as etree object

        New
        - in a previous version, this method was restricted to Object module
          (as Type).

        """
        xml = f"""
        <application 
            xmlns="http://www.zetcom.com/ria/ws/module/search" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.zetcom.com/ria/ws/module/search 
            http://www.zetcom.com/ria/ws/module/search/search_1_6.xsd">
            <modules>
              <module name="{Type}">
                <search limit="{limit}" offset="{offset}" />
              </module>
            </modules>
        </application>
        """

        q = Search(fromString=xml)
        q.validate(mode="search")
        # print(xml)
        r = await self.runSavedQuery(id=ID, mtype=Type, xml=q.toString())
        return etree.fromstring(r.content, ETparser)

    async def search(self, *, xml: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """
        Perform an ad-hoc search for modules items
        POST http://.../ria-ws/application/module/{mtype}/search/

        New: We're getting the mtype from the xml to avoid errors.
        """
        queryET = self.ETfromString(xml=xml)
        return await self._search(queryET=queryET, session=session)

    async def search2(self, *, query: Search, session: aiohttp.ClientSession) -> Module:
        """
        Perform a search, but with modern parameters and return values

        EXPECTS
        * Search object
        RETURNS
        * Module object
        """
        query.validate(mode="search")
        r = await self._search(queryET=query.toET(), session=session)
        m = Module(xml=await r.text())
        print (f"Length: {len(m)}")
        return m

    """
    B.3 WHOLE MODULE ITEMS
    r = createItem2(mtype="Object", data=m)
    m = getItem2(mtype="Object", ID=123)
    r = updateItem2(mtype="Object", data=m)
    r = deleteItem2(mtype="Object", ID=123)
    """

    async def getItem(self, *, module: str, id: int) -> aiohttp.ClientResponse:
        """
        Get a single module item
        GET http://.../ria-ws/application/module/{module}/{__id}

        URL parameters (e.g. &loadAttachment=true):

        loadAttachment | type: boolean | async default: false | load attachment if exists
        loadThumbnailExtraSmall | type: boolean | async default: false | load extra small thumbnail
        loadThumbnailSmall | type: boolean | async default: false | load small thumbnail
        loadThumbnailMedium | type: boolean | async default: false | load medium thumbnail
        loadThumbnailLarge | type: boolean | async default: false | load large thumbnail
        loadThumbnailExtraLarge | type: boolean | async default: false | load extra large thumbnail
        """
        return await self._get(f"{self.appURL}/module/{module}/{id}")

    async def getItem2(self, *, mtype: str, ID: int) -> Module:
        """
        Like getItem, but with modern parameter names and returns Module
        object.
        """
        r = await self.getItem(module=mtype, id=ID)
        return Module(xml=r.text)

    async def createItem(self, *, module: str, xml: str) -> aiohttp.ClientResponse:
        """
        Create new module item or items.
        POST http://.../ria-ws/application/module/{module}

        Is there a return value? I would like to know the id

        Zetcom writes: Create new module items. Adds module items including repeatable
        groups and references. The method returns a small response including the
        identifier of the newly created module item. Limitation on the size of the
        request may apply depending of the application container configuration.
        """
        url = self.appURL + "/module/" + module
        return await self._post(url, data=xml)

    async def createItem2(self, *, mtype: str, data: Module) -> Module:
        """
        Like createItem, but with modern parameters and returns Module
        object.

        DESIGN
        - Alternatively, createItem2 could expect xml as etree, but that wouldn't
          save anything, so no.

        r = self.createItem(module=mtype, xml=data.toString())
        data.toString()
        """
        # why are we not using Module's toString method?
        ET = data.toET()
        xml = etree.tostring(ET, pretty_print=True)
        r = await self.createItem(module=mtype, xml=xml)
        return Module(xml=r.text)

    async def updateItem(self, *, module: str, id: int, xml: str) -> aiohttp.ClientResponse:
        """
        Updates a module item with all of its fields and references.

        PUT http://.../ria-ws/application/module/{module}/{__id}
        """
        url = f"{self.appURL}/module/{module}/{id}"
        return await self._put(url, data=xml)

    async def updateItem2(self, *, mtype: str, ID: int, data: Module) -> aiohttp.ClientResponse:
        """
        v2 with other param names and data provided as Module object.
        """
        xml = data.toString()
        print(xml)
        return await self.updateItem(module=mtype, id=ID, xml=xml)

    async def deleteItem(self, *, module: str, id: int) -> aiohttp.ClientResponse:
        """
        Delete a module item
        DELETE http://.../ria-ws/application/module/{module}/{__id}

        What is the return value?
        """
        url = f"{self.appURL}/module/{module}/{id}"
        return await self._delete(url)

    async def deleteItem2(self, *, mtype: str, ID: int) -> aiohttp.ClientResponse:
        """What is the return value?"""
        return await self.deleteItem(module=mtype, id=ID)

    """
    B.4 FIELDs
    r = updateField2(mtype="Object", ID=123, dataField="OgrNameTxt", value="ein Titel")

    Read: Just get all fields instead of one or mark fields you want in the search to 
    get mostly that one.
    
    How do I create and delete a field? There don't seem to be specific endpoints
    """

    async def updateField(
        self, *, module: str, id: int, dataField: str, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Update a single field of a module item
        PUT http://.../ria-ws/application/module/{module}/{__id}/{dataField}

        NB: We don't need a createField method since simple dataFields are always created.
        """
        url = f"{self.appURL}/module/{module}/{id}/{dataField}"
        return await self._put(url, data=xml)

    async def updateField2(
        self, *, mtype: str, ID: int, dataField: str, value: str
    ) -> aiohttp.ClientResponse:
        """Higher order version of updateField which creates its own xml

        Note according to newer practice ID is spelled with capital letters
        here on purpose and parameter ´module´ is referred here as mtype as it
        is less ambiguous.

        Does the API return anything meaningful? HTTP response, I guess
        """

        m = Module()
        mm = m.module(name=mtype)
        item = m.moduleItem(parent=mm, ID=ID)
        m.dataField(parent=item, name=dataField, value=value)
        m.validate()
        # m.toFile(path="upField.debug.xml")  # needs to go later
        return await self.updateField(
            module=mtype, id=ID, dataField=dataField, xml=m.toString()
        )

    """
    B.5 REPEATABLE GROUPS and module|vocabulary references
    createReference (no createReference2 yet)
    createRepeatableGroup 
    """

    async def createReference(
        self,
        *,
        module: str,
        id: int,
        groupId: int,
        reference: str,
        repeatableGroup: str,
        xml: str,
    ) -> aiohttp.ClientResponse:
        """
        Zetcom says: "Add a reference to a reference field within a repeatable group".
        I am not sure what that means. But the example is described as follows: "Add a
        repeatable group item of type AdrContactGrp to an Address with ID 29011." So
        I think this adds/creates a ReferenceItem or repeatableGroupItem to an existing
        reference or repeatableGroup. Or it creates a reference inside a rGrp which is more
        like looking at the example. Then it should be called 'createReferenceInsideGroup'.

        POST http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup}/{__groupId}/{reference}

        Remember that the xml is different during downloads than for uploads.
        Upload xml omitts, for example, formattedValues.

        Zetcom's example is

        <application xmlns="http://www.zetcom.com/ria/ws/module">
          <modules>
            <module name="Address"> --> module
              <moduleItem id="29011"> --> __id
                <repeatableGroup name="AdrContactGrp"> --> rGrp
                  <repeatableGroupItem> --> no groupId
                    <dataField name="ValueTxt">
                      <value>max_muster</value>
                    </dataField>
                    <vocabularyReference name="TypeVoc"> -> reference
                      <vocabularyReferenceItem id="30158" />
                    </vocabularyReference>
                  </repeatableGroupItem>
                </repeatableGroup>
              </moduleItem>
            </module>
          </modules>
        </application>
        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}/{groupId}/{reference}"
        return await self._post(url, data=xml)

    async def createReferenceItem2(
        self, *, mtype: str, ID: int, rGrp: str, groupId: int, reference: str
    ):
        return self.createReference(
            module=mtype, id=ID, reference=reference, groupId=groupId
        )

    async def createRepeatableGroup(
        self, *, module: str, id: int, repeatableGroup: str, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Z: Create a new repeatable group or reference and add it to the module item specified by
        the {__id}
        MM: Create a new rGrpItem to an existing rGrp or new vRefItem/mRefItem to existing
        references.

        POST http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup|reference}

        Zetcom's example: Add a repeatable group item of type AdrContactGrp to an Address with
        ID 29011.

        eg. https://<host>/<application>/ria-ws/application/module/Address/29011/AdrContactGrp
        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}"
        return self._post(url, data=xml)

    async def createGrpItem2(
        self, *, mtype: str, ID: int, grpref: str, xml: str
    ) -> aiohttp.ClientResponse:  # nodes is lxml.etree
        """
        I believe this creates a new rGrpItem for an existing rGrp or an vRefItem/mRefItem for
        existing references.

        The first time I am using this, xml as str is more convenient than ET's nodes. Let's see
        if this is the case in the future as well.
        """
        # xml = etree.tostring(node)
        return self.createRepeatableGroup(
            module=mtype, id=ID, repeatableGroup=grpref, xml=xml
        )

    async def addModRefItem(
        self, *, mtype: str, modItemId: int, refName: str, refId: int
    ) -> aiohttp.ClientResponse:
        """
        Add a moduleReferenceItem to an existing moduleReference.

        Not sure if it already is sufficiently abstracted to work everywhere we need
        to add mRefItems.

        Untested.
        Ideally, we'll make a version that accepts multiple refIds -> v3?
        """

        xml = f"""<application xmlns="http://www.zetcom.com/ria/ws/module">
          <modules>
            <module name="{mtype}">
              <moduleItem id="{modItemId}">
                <moduleReference name="{refName}" targetModule="Object" multiplicity="M:N">
                  <moduleReferenceItem moduleItemId="{refId}"/>
                </moduleReference>
              </moduleItem>
            </module>
          </modules>
        </application>"""
        return self.createGrpItem2(mtype=mtype, ID=modItemId, grpref=refName, xml=xml)

    async def addModRefItem3(
        self, *, mType: str, modItemId: int, refName: str, refIds: list
    ) -> aiohttp.ClientResponse:
        """
        Like addModRefItem2, but accepts a list of refIds. Untested.
        """
        xml = f"""<application xmlns="http://www.zetcom.com/ria/ws/module">
          <modules>
            <module name="{mtype}">
              <moduleItem id="{modItemId}">
                <moduleReference name="{refName}" targetModule="Object" multiplicity="M:N"/>
              </moduleItem>
            </module>
          </modules>
        </application>"""
        docN = ET.xml(xml)
        modRef = docN.xpath("//m:moduleReference", namespaces=NSMAP)[0]
        for refId in refIds:
            etree.SubElement(
                modRef, "m:moduleReferenceItem", {"moduleItemId": refId}, NSMAP
            )
        return self.createGrpItem2(
            mtype=mType, ID=modItemId, grpref=refName, xml=docN.tostring()
        )  # encoding="UTF8"

    async def updateRepeatableGroup(
        self, *, module: str, id: int, referenceId: int, repeatableGroup: str, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Update all fields of repeatable groups / references
        PUT http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup|reference}/{__referenceId}

        me: It looks like this is NOT updating all fields of a repeatable group, but all fields of rGrpItem
        instead.

        PUT http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup|reference}/{__repeatableGroupId|__referenceId}

        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}/{referenceId}"
        # xml = xml.encode()
        return self._put(url, data=xml)

    async def updateRepeatableGroup2(
        self, *, mtype: str, ID: int, referenceId: int, repeatableGroup: str, node
    ) -> aiohttp.ClientResponse:
        """
        Deprecated version. Please use updateRepeatableGroupItem2 instead
        """
        xml = etree.tostring(node)
        return updateRepeatableGroup(
            module=mtype,
            id=ID,
            referenceId=referenceId,
            repeatableGroup=repeatableGroup,
            xml=xml,
        )

    async def updateRepeatableGroupItem2(
        self, *, mtype: str, ID: int, referenceId: int, repeatableGroup: str, node
    ) -> aiohttp.ClientResponse:
        xml = etree.tostring(node)
        return updateRepeatableGroup(
            module=mtype,
            id=ID,
            referenceId=referenceId,
            repeatableGroup=repeatableGroup,
            xml=xml,
        )

    async def updateFieldInGroup(
        self,
        *,
        module: str,
        id: int,
        referenceId: int,
        dataField: str,
        repeatableGroup: str,
        xml: str,
    ) -> aiohttp.ClientResponse:
        """
        Update a single data field of a repeatable group / reference
        PUT http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup|reference}/{__referenceId}/{dataField}
        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}/{referenceId}/{dataField}"
        return self._put(url, data=xml)

    async def deleteRepeatableGroup(
        self, *, module: str, id: int, referenceId: str, repeatableGroup: str
    ) -> aiohttp.ClientResponse:
        """
        Delete a complete repeatable group / reference
        DELETE http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup|reference}/{__referenceId}
        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}/{referenceId}"
        return self._delete(url)

    async def deleteReferenceInGroup(
        self,
        *,
        module: str,
        id: int,
        groupId: str,
        reference: str,
        referenceId: int,
        repeatableGroup: str,
    ) -> aiohttp.ClientResponse:
        """
        Delete a reference contained within a repeatable group
        DELETE http://.../ria-ws/application/module/{module}/{__id}/{repeatableGroup}/{__groupId}/{reference}/{__referenceId}
        """
        url = f"{self.appURL}/module/{module}/{id}/{repeatableGroup}/{groupId}/{reference}/{referenceId}"
        return self._delete(url)

    #
    # C ATTACHMENTs AND THUMBNAILs
    #

    async def getAttachment(self, *, module: str, id: int) -> aiohttp.ClientResponse:
        """
        GET http://.../ria-ws/application/module/{module}/{__id}/attachment
        Get an attachment for a specified module item. You should use the GET method with
        application/octet-stream Header (described below) that returns the attachment in
        binary form. The binary file format approach consumes less bandwidth than the xml
        approach cause of the Base64 encoding that becomes necessary with xml.

        The request will return status code 200 (OK) if the syntax of the request was
        correct. The content will be send using the MIME type application/octet-stream
        and the Content-disposition header with a suggested filename.

        """

        url = f"{self.appURL}/module/{module}/{id}/attachment"
        r = self._get(url, headers={"Accept": "application/octet-stream"})
        return r

    async def saveAttachment(self, *, module: str = "Multimedia", id: int, path: str) -> int:
        """
        Streaming version of getAttachment that saves attachment directly to disk.

        Expects
        - module: module type (e.g. "Multimedia"),
        - id: item id in specified module (int)
        - path: filename/path to save attachment to
        to.

        Returns id if successful.

        Note: There is a similar saveAttachments in Sar.py that calls this one.
        """
        url = f"{self.appURL}/module/{module}/{id}/attachment"
        r = self._get(url, stream=True, headers={"Accept": "application/octet-stream"})
        r.raise_for_status()  # todo: replace with r.raise_for_status()?

        # exception: not using _get
        with self.session.get(
            url, stream=True, headers={"Accept": "application/octet-stream"}
        ) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        # self.headers["Accept"] = oldAccept
        return id

    async def getThumbnail(self, *, module: str, id: int, path: str) -> aiohttp.ClientResponse:
        """
        Get the thumbnail of a module item attachment
        GET http://.../ria-ws/application/module/{module}/{__id}/thumbnail
        """
        url = f"{self.appURL}/module/{module}/{id}/thumbnail"
        r = self._get(url, headers={"Accept": "application/octet-stream"})
        return r  # r.content

    async def updateAttachment(self, *, module: str, id: int, path: str) -> aiohttp.ClientResponse:
        """
        Add or update the attachment of a module item, as a base64 encoded XML
        Add or update the attachment of a module item, as a binary stream
        PUT http://.../ria-ws/application/module/{module}/{__id}/attachment
        Untested
        """
        url = f"{self.appURL}/module/{module}/{id}/attachment"
        with open(path, mode="rb") as f:
            file = f.read()
        return self._put(url, data=file)

    async def deleteAttachment(self, *, module: str, id: int) -> aiohttp.ClientResponse:
        """
        Delete the attachment of a module item
        DELETE http://.../ria-ws/application/module/{module}/{__id}/attachment
        """
        url = f"{self.appURL}/module/{module}/{id}/attachment"
        return self._delete(url)

    #
    # D RESPONSE orgunit
    #
    async def getOrgUnits(self, *, module: str) -> aiohttp.ClientResponse:
        """
        Get the list of writable orgUnits for a module
        GET http://.../ria-ws/application/module/{module}/orgunit
        Response body async definition: orgunit_1_0.xsd
        """
        url = f"{self.appURL}/module/{module}/orgunit"
        return self._get(url)

    async def getOrgUnits2(self, *, mtype: str) -> Module:
        r = self.getOrgUnits(module=mtype)
        return Module(xml=r.text)

    #
    # EXPORT aka report -> LATER
    #

    async def listReports(self, module: str) -> aiohttp.ClientResponse:
        """
        Get a list of available exports / reports for a module
        GET http://.../ria-ws/application/module/{module}/export

        Response body async definition: export_1_0.xsd

        The request will return status code 200 (OK) if the request was correct and
        there is at least one export available. If there is no export for the module
        status code 204 (No Content) will be returned.
        """
        return self._get(f"{self.appURL}/module/{module}/export")

    async def reportModuleItem(
        self, *, module: str, itemId: int, exportId: int
    ) -> aiohttp.ClientResponse:
        """
        Export a single module item via the reporting system
        GET http://.../ria-ws/application/module/{module}/{__id}/export/{id}
        Response header:
            Content-Type: application/octet-stream
            Content-Disposition: attachment;filename={random-file-name}.{export-specific-file-extension}
        """
        url = f"{self.appURL}/module/{module}/{itemId}/export/{exportId}"
        r = self._get(url, headers={"Accept": "application/octet-stream"})
        return r

    async def reportModuleItems(self, *, module: str, id: int, xml: str) -> aiohttp.ClientResponse:
        """
        Export multiple module items via the reporting system
        POST http://.../ria-ws/application/module/{module}/export/{id}
        """
        url = f"{self.appURL}/module/{module}/export/{id}"
        return self._post(url, data=xml)

    #
    # NEW NATIVE VOCABULARY MODULE (can also do json)
    #
    # Labels, Node Classes, TermClassLabel, Node, Term, nodeParents, nodeRelations
    # get, add, delete and sometimes update
    #
    async def vInfo(self, *, instanceName: str, id: int = None) -> aiohttp.ClientResponse:
        """
        Shows the vocabulary instance information for the give vocabulary.
        GET http://.../ria-ws/application/vocabulary/instances/{instanceName}
        GET http://../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{__id}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}"
        if id is not None:
            url = url + "/nodes/{id}"
        return self._get(url)

    async def vGetNodes(
        self,
        *,
        instanceName: str,
        offset: int = 0,
        limit: int = 100,
        termContent: str = None,
        status: str = None,
        nodeName: str = None,
    ) -> aiohttp.ClientResponse:
        """
        The request returns available vocabulary nodes of the vocabulary instance.
        GET http://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/search
        Todo: json response
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/search"
        params: dict[str, Union[int, str]] = {
            "offset": offset,
            "limit": limit,
        }  # dict[str, int, str]
        # for each in termContent, status, nodeName:
        if termContent is not None:
            params["termContent"] = termContent
        if status is not None:
            params["status"] = status
        if nodeName is not None:
            params["nodeName"] = nodeName
        return self._get(url, headers=params)

    async def vUpdate(self, *, instanceName: str, xml: str) -> aiohttp.ClientResponse:
        """
        Update Vocabulary Instance
        PUT https://.../ria-ws/application/vocabulary/instances/{instanceName}
        Request body async definition: instance element from vocabulary_1_1.xsd
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}"
        return self._put(url, data=xml)

    # LABELS
    async def vGetLabels(self, *, instanceName: str) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Instance labels
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/labels
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/labels"
        return self._get(url)

    async def vAddLabel(self, *, instanceName: str, xml: str) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Instance label
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/labels
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/labels"
        return self._post(url, data=xml)

    async def vDelLabel(self, *, instanceName: str, language: str) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Instance label
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/labels/{language}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/labels/{language}"
        return self._delete(url)

    # NODE CLASSES
    async def vGetNodeClasses(self, *, instanceName: str) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Instance Node Classes
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses"
        return self._get(url)

    async def vAddNodeClass(self, *, instanceName: str, xml: str) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Instance Node Class
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses"
        return self._post(url, data=xml)

    async def vAddNodeClassLabel(
        self, *, instanceName: str, className: str, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Instance Node Class Label
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses/{className}/labels
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses/{className}/labels"
        return self._post(url, data=xml)

    async def vDelNodeClassLabel(
        self, *, instanceName: str, className: str, language: str
    ) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Instance Node Class Label
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses/{className}/labels/{language}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses/{className}/labels/{language}"
        return self._delete(url)

    async def vDelNodeClass(self, *, instanceName: str, className: str) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Instance Node Class
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses/{className}
        """
        url = (
            f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses/{className}"
        )
        return self._delete(url)

    # TERM CLASSES
    async def vGetTermClasses(self, *, instanceName: str) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Instance Term Classes
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/termClasses
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/termClasses"
        return self._get(url)

    async def vAddTermClass(self, *, instanceName: str, xml: str) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Instance Term Class
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/termClasses
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/termClasses"
        return self._post(url, data=xml)

    async def vDelTermClass(self, *, instanceName: str, className: str) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Instance Term Class
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodeClasses/{className}
        """
        url = (
            f"{self.appURL}/vocabulary/instances/{instanceName}/nodeClasses/{className}"
        )
        return self._delete(url, data=xml)

    # TermClassLabel
    async def vAddTermClassLabel(
        self, *, instanceName: str, className: str, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Instance Term Class Label
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/termClasses/{className}/labels
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/termClasses/{className}/labels"
        return self._post(url, data=xml)

    async def vDelTermClassLabel(
        self, *, instanceName: str, className: str, language: str
    ) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Instance Term Class Label
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/termClasses/{className}/labels/{language}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/termClasses/{className}/labels/{language}"
        return self._delete(url)

    # vocabularyNode
    async def vNodeByIdentifier(self, *, instanceName: str, id: int) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Node by identifier
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{id}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{id}"
        return self._get(url)

    async def vAddNode(self, *, instanceName: str, xml: str) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Node
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes"
        return self._post(url, data=xml)

    async def vDelNode(self, *, instanceName: str, id: int) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Node
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{id}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{id}"
        return self._delete(url)

    async def vUpdateNode(self, *, instanceName: str, id: int, xml: str) -> aiohttp.ClientResponse:
        """
        Update Vocabulary Node
        PUT https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{id}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{id}"
        return self._put(url, data=xml)

    # TERM
    async def vAddTerm(
        self, *, instanceName: str, nodeId: int, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Term
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms"
        return self._post(url, data=xml)

    async def vUpdateTerm(
        self, *, instanceName: str, nodeId: str, termId: int, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Update Vocabulary Term
        PUT https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms/{termId}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms/{termId}"
        return self._put(url, data=xml)

    async def vDelTerm(
        self, *, instanceName: str, nodeId: int, termId: int
    ) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Term
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms/{termId}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/terms/{termId}"
        return self._delete(url)

    # NodeParent
    async def vNodeParents(self, *, instanceName: str, nodeId: int) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Node Parents / async default Node Relations
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents/
        (I am assuming the trailing slash is a typo.)
        """
        url = (
            f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents"
        )
        return self._delete(url)

    async def vAddNodeParent(
        self, *, instanceName: str, nodeId: int, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Node Parent / async default Node Relations
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents/
        """
        url = (
            f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents"
        )
        return self._post(url, data=xml)

    async def vDelNodeParent(
        self, *, instanceName: str, nodeId: int, parentNodeId: int
    ) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Node Parent / async default Node Relations
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents/{parentNodeId}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents/{parentNodeId}"
        return self._delete(url)

    # nodeRelations
    async def vNodeRelations(self, *, instanceName: str, nodeId: int) -> aiohttp.ClientResponse:
        """
        Get Vocabulary Node Relations / Advanced Node Relations
        GET https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/relations/
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/relations"
        return self._get(url)

    async def vAddNodeRelation(
        self, instanceName: str, nodeId: int, xml: str
    ) -> aiohttp.ClientResponse:
        """
        Add Vocabulary Node Relation / Advanced Node Relations
        POST https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/relations/
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}/relations"
        return self._post(url, data=xml)

    async def vDelNodeRelation(
        self, instanceName: str, nodeId: int, relationId: str
    ) -> aiohttp.ClientResponse:
        """
        Delete Vocabulary Node Relation / Advanced Node Relations
        DELETE https://.../ria-ws/application/vocabulary/instances/{instanceName}/nodes/{nodeId}/parents/{relationId}
        """
        url = f"{self.appURL}/vocabulary/instances/{instanceName}/nodes/{nodeId}parents/{relationId}"
        return self._delete(url)

    #
    # HELPERS
    #

    async def ETfromString(self, *, xml: str) -> etree:
        return etree.fromstring(bytes(xml, "UTF-8"))

    async def toFile(self, *, xml: str, path: str) -> None:
        with open(path, "w", encoding="UTF-8") as f:
            f.write(xml)

    async def completeXML(self, *, fragment: str) -> str:
        """
        Expects a moduleItem as xml string, returns a whole
        document as xml string.
        """

        whole = f"""
        <application xmlns="http://www.zetcom.com/ria/ws/module">
            <modules name="Object">
                <moduleItem>
                    {fragment}
                </moduleItem>
            </modules>
        </application>
        """
        return whole




if __name__ == '__main__':
    """
    Sollen wir vielleicht ein realistisches Beispiel nehmen? Also eine Gruppe nehmen und dann getPack nachbauen?
    
    getPack group
    """
    
    async def getByChunk(): pass
    """
    Should we chunk by default? In asynchronous it's an opportunity for parallel requests rather than a liability...
    """
    
    async def getByApproval(): pass
    
    
    async def main(): #getByType
        auth = aiohttp.BasicAuth(login=user, password=pw)
        headers =             {
                "Content-Type": "application/xml",
                "Accept": "application/xml;charset=UTF-8",
                "Accept-Language": "de",
            }

        fields: dict = {
            "exhibit": {
                "Multimedia": "MulObjectRef.ObjRegistrarRef.RegExhibitionRef.__id",
                "Object": "ObjRegistrarRef.RegExhibitionRef.__id",
                "Person": "PerObjectRef.ObjRegistrarRef.RegExhibitionRef.__id",
                "Registrar": "RegExhibitionRef.__id",
            },
            "group": {
                "Multimedia": "MulObjectRef.ObjObjectGroupsRef.__id",
                "Object": "ObjObjectGroupsRef.__id",
                "Person": "PerObjectRef.ObjObjectGroupsRef.__id",
            },
            "loc": {
                "Multimedia": "MulObjectRef.ObjCurrentLocationVoc",
                "Object": "ObjCurrentLocationVoc",
                "Person": "PerObjectRef.ObjCurrentLocationVoc",
                },
        }

        async with aiohttp.ClientSession(auth=auth, raise_for_status=True, headers=headers) as session:
            c = Client(user=user, pw=pw, baseURL=baseURL)
            tasks = []

            for mtype in ["Multimedia", "Object", "Person"]: 
                q = Search(module=mtype)
                q.addCriterion(
                    field=fields["group"][mtype],
                    operator="equalsField",
                    value=str(154397),
                )
                tasks.append(asyncio.ensure_future(c.search2(query=q, session=session)))

            rL = await asyncio.gather(*tasks)            
            m = Module()
            print ("Adding results...")
            for item in rL:
                m += item    
            print (f"total length: {len(m)}")
            m.toFile(path="async.xml")

    
    #https://stackoverflow.com/questions/45600579    
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    
    #todo add baseURL to ClientSession


    #c = Client(user=user, pw=pw, baseURL=baseURL)
    #await c.open_session()
        #await asyncio.sleep(1)
    #await c.close_session()

    # asyncio.run(main())
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(main())
    #loop.close()
    
