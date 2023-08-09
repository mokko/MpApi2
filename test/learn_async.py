"""
We need to limit the amount of requests. Possible devices to achieve a limit seem to be a 
semaphore or queue. We also need to handle exceptions. We probably DON'T need a timeout.
"""

import asyncio
import pytest
import random


@pytest.mark.asyncio
async def roll_die() -> int:
    return random.randrange(1, 7)


async def roll_die2(sem) -> int:
    async with sem:
        res = random.randrange(1, 7)
        # print(res)


@pytest.mark.asyncio
async def caller() -> None:
    """
    Call a single coro
    """
    result = await roll_die()
    assert result < 7
    print(f"{result}")


async def caller1() -> None:
    """
    Call many coros sequentially
    """
    for n in range(100):
        r = await roll_die()
        # print(f"{n}:{r}")


async def caller2(*, total: int) -> None:
    """
    Call many coros asynchronously
    """
    coros = list()
    for n in range(total):
        coros.append(roll_die())
        # await asyncio.sleep(0.01)

    results = await asyncio.gather(*coros)
    n = 1  # we create new n here, hence the order
    for result in results:
        # print(f"{n}:{result}")
        n += 1


async def caller3(*, total: int) -> None:
    """
    Call many coros asynchronously with a limit
    Pass a total > limit.
    """
    sem = asyncio.Semaphore(3)
    coros = list()
    for n in range(total):
        coros.append(roll_die2(sem))
    await asyncio.gather(*coros)


def test_zero() -> None:
    asyncio.run(caller())


def test_one() -> None:
    asyncio.run(caller1())


def test_two() -> None:
    asyncio.run(caller2(total=100))


def test_main() -> None:
    asyncio.run(caller3(total=100))


def test_queue() -> None:
    asyncio.Queue(maxsize=0)
