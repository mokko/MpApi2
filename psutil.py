import psutil
from pprint import pprint as pp

for p in psutil.process_iter(["name", "status"]):
    if p.info["status"] == psutil.STATUS_RUNNING:
        if p.name() == "python.exe":
            print(p)
