"""
monk - a cli frontend to MpApi.aio and asynchronous replacement for mink to download 
    data from MuseumPlus.

CLI USAGE
    monk -j jobname # expect jobs.dsl in pwd

DSL format
    .oonf: # optional
        chunkSize 1000 # optional, defaults to 1000
        modules Artist, Multimedia Object # optional, defaults to Artist, Multimedia and Object
        
    test:
        apack approval 12345 # comment
        apack exhibit 12344 # comment
        apack group 12345 # comment
        apack loc 12345 # comment
        apack query 12345 # comment

    On first level of indentation there are labels. Labels have to end in colon. There
    are two kinds of labels: (a) config values or (b) job labels.

    We require config values to be located before job labels, but we don't test that.

    On second level of indentation expect commands. Currently, there is only one command:
    apack - which gets records from several modules (as defined in conf/modules).
    
    apack stands for asynchronous pack. It always chunks and zips the chunks. Since
    behavior differs from mink, we use a new keyword. apack expects one of the following
    types: approval, exhibit, group, loc, query and a corresponding ID.


USAGE
    client = Monk(conf_fn="jobs.dsl")
    client.run_job("testjob")
    # todo: some parameter to limit the number of simultaneous connections
    # todo: chunkSize parameter

    #different methods for every pack type?
    async for chunk in client.packPerGroup(ID="12345"):  
    async for chunk in client.packPerLoc(ID="12345"):  
    async for chunk in client.packPerApproval(ID="12345"):  
    async for chunk in client.packPerQuery(ID="12345", target="Object"):   
    async for chunk in client.packPerExhibit(ID="12345"):  

    #one method with lots of types?
    async for chunk in client.getPack(type="group", ID="12345"): # target="Object" 
        # chunking by default and non-optional; 
        # should we chunk knowing how many chunks we're expecting this time?
        # pack types are group, loc, exhibit, approval, [saved] query
        # do we need a target mtype for query?
        chunk.toFile(path="path/to/file.xml")

    async for attachment in client.getAttachments(type="group", ID="12345"):
        attachment.path(Path)

    q = Search(module="Object")
    ...
    M = await client.search(query=q)


"""

import aiohttp
import asyncio

import datetime
#import logging
import MpApi.aio.client as client
from pathlib import Path
import sys
from typing import List, Any

acceptLang = "de"
allowed_commands = ["attachments", "chunk", "getItem", "getPack", "pack"]
maxConnections = 30  # simultaneous connections

# I dont remember what all does;
# in monk everything is chunked so chunk and getPack synonyms
# chunks dont require filenames so the respective label in getPack gets ignored

class ConfigError(Exception): pass


class Monk:
    def __init__(self, *, conf_fn: str = "jobs.dsl") -> None:
        self.conf_fn = conf_fn
        self.chunkSize = 1000 # default, can be overwritten from jobs.dsl
        self.modules = ['Artist', 'Multimedia', 'Object']
        from mpapi.constants import get_credentials
        self.user,self.pw,self.baseURL = get_credentials()
 
    def run_dsl(self, *, job:str) -> None:
    """
    Parse the dsl file at self.conf_fn and run the provided job.
    """
        with open(self.conf_fn, mode="r") as file:
            c = 0  # line counter
            for line in file:
                c += 1
                line = line.expandtabs(4)  # let's use spaces internally
                uncomment = line.split("#", 1)[0].strip()
                if uncomment.isspace() or not uncomment:
                    continue
                indent_lvl = int((len(line) - len(line.lstrip()) + 4) / 4)
                parts: list[str] = uncomment.split()
                if indent_lvl == 1:  # job label or conf
                    if not parts[0].endswith(":"):
                        raise ConfigError(
                            f"Label doesn't end with colon: line {c} {parts[0]}"
                        )
                    current_label = parts[0][:-1]
                    if current_label == job:
                        active_job = True
                    else:
                        active_job = False

                    if current_label.lower() == ".conf":
                        inside_conf = True
                    else:
                        inside_conf = False
                else:
                    if inside_conf:
                        if parts[0] == "chunkSize":
                            self.chunkSize = int(parts[1])
                        elif parts[0] == "modules":
                            self.modules = []
                            for each in parts[1:]:
                                each = strip(each).replace(',','')
                                self.modules.push(each)
                        else:
                            print ("WARNING:Ignoring unknown config value {parts[0]}")
                        
                    if active_job:
                        if parts[0] == "apack":
                            self.apack(Type=parts[1], ID=int(parts[2]))
                        else:
                            print ("WARNING:Ignoring unknown command keyword {parts[0]}")

    def apack (self, *, Type:str, ID:int):
        """
        We need to get data for every module mentioned in self.modules. We need to chunk the 
        responses and save results as zip file. This time we want deterministic chunks, so we
        can schedule several requests simultaneously (asychronously).
        
        Where do we save stuff to? In project_dir, i.e. cwd
        jobname/YYYMMDD/query-1234-chunk1.zip
        """

    #
    # helpers
    #
 
    def _project_dir(self, *, job: str) -> None:
        """
        makes project dir in cwd/job/YYYYMMDD

        There is no check that job really exists in jobs.dsl file, i.e. we're trusting
        the user.

        Side-effect: We're setting self.project_dir. I don't know that I really need it.
        """
        date: str = datetime.datetime.today().strftime("%Y%m%d")
        project_dir: Path = Path(job) / date
        if not project_dir.is_dir():
            Path.mkdir(project_dir, parents=True)
            # should raise error if file at that location
        return project_dir
