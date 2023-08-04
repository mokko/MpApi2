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
"""

import aiohttp
import asyncio

import datetime

# import logging
from mpapi.constants import get_credentials

# import MpApi.aio.client as client
from MpApi.aio.chunky import Chunky
from MpApi.aio.session import Session
from pathlib import Path

# import sys
# from typing import List, Any

acceptLang = "de"
allowed_commands = ["attachments", "chunk", "getItem", "getPack", "pack"]
maxConnections = 30  # simultaneous connections

# I dont remember what all does;
# in monk everything is chunked so chunk and getPack synonyms
# chunks dont require filenames so the respective label in getPack gets ignored


class ConfigError(Exception):
    pass


class Monk:
    def __init__(self, *, conf_fn: str = "jobs.dsl") -> None:
        self.conf_fn = conf_fn
        user, pw, baseURL = get_credentials()
        self.baseURL = baseURL
        self.user = user
        self.pw = pw
        
    async def apack(self, *, qtype: str, ID: int):
        """
        We need to get data for every module mentioned in self.modules. We need to chunk the
        responses and save results as zip file. This time we want deterministic chunks, so we
        can schedule several requests simultaneously (asychronously).

        Where do we save stuff to? In project_dir, i.e. cwd
        jobname/YYYMMDD/query-1234-chunk1.zip

        ask how many records in that group: group 1234
        plan the chunks; for each chunk
           get the objects
           find out which related target records in that data
        """

        chnkr = Chunky(
            chunk_size=self.chunk_size, 
        )

        print(f"apack with {qtype} {ID}")
        print(f"{self.chunk_size=}")

        async with Session(user=self.user, pw=self.pw, baseURL=self.baseURL) as session:
            # no of results; target is always Object?
            result_no = await chnkr.count_results(session=session, qtype=qtype, target="Object", ID=ID) 
            if not result_no:
                print("Nothing to download!")
                return

            #no of chunks
            chnk_no = int(result_no/chnkr.chunk_size)+1

            print(f"{chnk_no=}")
            for c in range(chnk_no):
                print (c)

        #get_objects(qtype="group", ID=1234)    
        # let the chunker return some chunks and save them here
        #for chunk in self.chunker.getByType(Type=Type, ID=ID):
        #    if chunk:  # a Module object
                # chunk_fn = self._chunkPath(Type=Type, ID=ID, no=no, suffix=".xml")
                # chunk.clean()
                # self.info(f"zipping chunk {chunk_fn}")
                # chunk.toZip(path=chunk_fn) # write zip file to disk
                # chunk.validate()
        #        pass

    def run_job(self, *, job: str) -> None:
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
                    #print(f"{current_label=} {job=}")
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
                            self.chunk_size = int(parts[1])
                        # elif parts[0] == "modules":
                            # chunker.modules = []
                            # for each in parts[1:]:
                                # each = each.strip().replace(",", "")
                                # chunker.modules.append(each)
                        else:
                            print(f"WARNING:Ignoring unknown config value '{parts[0]}'")

                    if active_job:
                        if parts[0] == "apack":
                            print("GH!")
                            asyncio.run(self.apack(qtype=parts[1], ID=int(parts[2])))
                        else:
                            print(f"WARNING:Ignoring unknown command keyword '{parts[0]}'")


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
