from MpApi.monk import Monk


def test_chunk_path():
    path = monk._chunk_path(qtype="group", ID=182397, cno=1, suffix=".zip")
    print("{path=}")
    assert path is True
