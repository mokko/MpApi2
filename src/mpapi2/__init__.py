"""An Unofficial Asynchronous Client for the MuseumPlus API"""

__version__ = "0.0.1"
credentials = "credentials.py"  # expect credentials in pwd
import argparse
from monk import Monk
#from pathlib import Path

if Path(credentials).exists():
    with open(credentials) as f:
        exec(f.read())


def monk():
    parser = argparse.ArgumentParser(description="Commandline frontend for MpApi.py")
    parser.add_argument("-j", "--job", help="job to run", required=True)
    parser.add_argument("-d", "--dsl", help="jobs file", default="jobs.dsl")
    args = parser.parse_args()

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    m = Monk(baseURL=baseURL, pw=pw, user=user)
    asyncio.run(m.execute_dsl(fn=args.dsl, job=args.job))
