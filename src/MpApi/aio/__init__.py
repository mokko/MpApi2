"""An Unofficial Asynchronous Client for the MuseumPlus API"""

__version__ = "0.0.1"
import argparse
import asyncio
from MpApi.aio.monk import Monk

# from pathlib import Path
# from mpapi.constants import get_credentials, NSMAP


def monk():
    parser = argparse.ArgumentParser(description="Commandline frontend for MpApi.py")
    parser.add_argument("-j", "--job", help="job to run", required=True)
    parser.add_argument(
        "-d",
        "--dsl",
        help="jobs file (optional, defaults to 'jobs.dsl')",
        default="jobs.dsl",
    )
    args = parser.parse_args()
    m = Monk(conf_fn=args.dsl)
    m.run_job(job=args.job)
