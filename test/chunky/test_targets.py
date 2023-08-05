from MpApi.aio.chunky import Chunky
from mpapi.constants import get_credentials
from mpapi.module import Module

user, pw, baseURL = get_credentials()


def test_init():
    chunker = Chunky(baseURL=baseURL, pw=pw, user=user)
    assert chunker
    print(chunker)


def test_analyze_related():
    source = "C:/m3/MpApi-Replacer/sdata/temp485397.zml.xml"
    m = Module(file=source)

    chunker = Chunky(baseURL=baseURL, pw=pw, user=user)
    L = chunker.analyze_related(data=m)
    print(L)
