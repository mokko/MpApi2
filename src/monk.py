"""
monk - an asynchronous drop in replacement for mink and frontend to MpApi

I am experimenting with a new asynchronous client that has learned from my experiments so far.

USAGE
    client = Monk(baseURL=baseURL, user=user, pw=pw)
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

# from asyncio import ClientSession
import datetime
import logging
from pathlib import Path
from typing import List, Any
from mpapi2 import MpApi2

acceptLang = "de"
allowed_commands = ["attachments", "chunk", "getItem", "getPack", "pack"]
limit = 30  # simultaneous connections

# I dont remember what all does;
# in monk everything is chunked so chunk and getPack synonyms
# chunks dont require filenames so the respective label in getPack gets ignored


class Monk:
    def __init__(self, *, baseURL: str, pw: str, user: str) -> None:
        # should we client with functions this time?
        # so we import it and dont save data in it
        self.auth = aiohttp.BasicAuth(login=user, password=pw)
        self.headers = {
            "Content-Type": "application/xml",
            "Accept": "application/xml;charset=UTF-8",
            "Accept-Language": acceptLang,
        }
        assert sys.version_info >= (3, 9), "requires Python 3.9+."

    async def execute_dsl(self, *, fn: str, job: str) -> None:
        self.project_dir = self.project_dir(job=job)
        self.init_log(pd=self.project_dir)

        # todo: experiment wih baseURL param in clientSession
        # self.appURL = baseURL + "/ria-ws/application"

        # making session only once
        async with aiohttp.ClientSession(
            auth=self.auth,
            raise_for_status=True,
            headers=self.headers,
            connector=aiohttp.TCPConnector(limit=maxConnections),
        ) as session:
            await self.parse_dsl(fn=fn, job=job)

    async def execute_line(self, *, cmd: str, args: List[Any]) -> None:
        tasks = []
        if cmd == "attachments":
            tasks.append(
                asyncio.ensure_future(self.attachments(ptype=args[0], ID=args[1]))
            )
        elif cmd == "getItem":
            tasks.append(asyncio.ensure_future(self.getItem(mtype=args[0], ID=args[1])))
        elif cmd == "chunk" or cmd == "getPack":
            tasks.append(asyncio.ensure_future(self.getPack(ptype=args[0], ID=args[1])))
        # elif cmd == "pack":
        #    tasks.append(asyncio.ensure_future(pack(query=q, session=session)))
        else:
            raise SyntaxError(f"ERROR: Command {cmd} not recogized")

    async def attachments(self, *, ptype: str, ID: int):
        pass

    async def getItem(self, *, mtype: str, ID: int):
        out_fn = self.project_dir / f"{mtype}{ID}.xml"
        if out_fn.exists():
            self.info(f"getItem {out_fn} from cache")
            return Module(file=out_fn)
        else:
            self.info(f"getItem {out_fn} from remote")
            m = await client.getItem2(mtype=mtype, ID=ID)
            m.toFile(path=out_fn)
            return m

    async def getPack(self, *, ptype: str, ID: int):
        pass

    def info(self, msg: str) -> None:
        logging.info(msg)
        print(msg)

    def init_log(self, pd: Path) -> None:
        logging.basicConfig(
            datefmt="%Y%m%d %I:%M:%S %p",
            filename=pd / "monk.log",
            filemode="a",  # append now since we're starting a new folder
            # every day now anyways.
            level=logging.DEBUG,
            format="%(asctime)s: %(message)s",
        )
        print(f"Project dir: {pd}")

    async def parse_dsl(self, *, fn: str, job: str) -> None:
        any_job = False
        right_job = True
        with open(fn, mode="r") as file:
            c = 0  # line counter
            for line in file:
                c += 1
                line = line.expandtabs(4)  # let's use spaces internally
                uncomment = line.split("#", 1)[0].strip()
                if uncomment.isspace() or not uncomment:
                    continue
                indent_lvl = int((len(line) - len(line.lstrip()) + 4) / 4)
                parts: list[str] = uncomment.split()
                if indent_lvl == 1:  # job label
                    if not parts[0].endswith(":"):
                        raise SyntaxError(
                            f"Job label has to end with colon: line {c} {parts[0]}"
                        )
                    current_job = parts[0][:-1]
                    if current_job == job:
                        any_job = True
                        right_job = True
                    else:
                        right_job = False
                    # continue # really necessary?
                elif indent_lvl == 2:
                    cmd: str = parts[0]
                    if len(parts) > 1:
                        args = parts[1:]
                    else:
                        args = []
                    if right_job is True:
                        print(f"**{cmd} {args}")
                        await self.execute_line(cmd=cmd, args=args)
                elif indent_lvl > 2:
                    print(f"indent lvl: {indent_lvl} {parts}")
                    raise SyntaxError("ERROR: Too many indents in dsl file")

        if any_job is False:
            raise ValueError(
                "ERROR: User-supplied job didn't match any job from the definition file!"
            )

    def project_dir(self, *, job: str) -> None:
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


if __name__ == "__main__":
    credentials = "credentials.py"  # expect credentials in pwd
    import argparse

    # from monk import Monk

    if Path(credentials).exists():
        with open(credentials) as f:
            exec(f.read())

    parser = argparse.ArgumentParser(description="Commandline frontend for MpApi.py")
    parser.add_argument("-j", "--job", help="job to run", required=True)
    parser.add_argument("-d", "--dsl", help="jobs file", default="jobs.dsl")
    args = parser.parse_args()

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    m = Monk(baseURL=baseURL, pw=pw, user=user)
    asyncio.run(m.execute_dsl(fn=args.dsl, job=args.job))
