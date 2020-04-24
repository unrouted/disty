import logging

from distribd.machine import Machine
from distribd.storage import Storage


async def test_write_term(tmp_path):
    with open(tmp_path / "term", "w") as fp:
        fp.write("10")

    storage = Storage(tmp_path / "journal")
    await storage.open()

    # Assert term restored from disk
    assert storage.current_term == 10

    # Assert can move term forward
    await storage.write_term(20)
    assert storage.current_term == 20

    # Assert log can't rewind
    await storage.write_term(15)
    assert storage.current_term == 20

    # Assert term saved to disk
    with open(tmp_path / "term", "r") as fp:
        assert fp.read() == "20"

    await storage.close()


async def test_rollback(tmp_path):
    with open(tmp_path / "term", "w") as fp:
        fp.write("10")

    storage = Storage(tmp_path / "journal")
    await storage.open()

    for i in range(10):
        await storage.commit(10, {"tid": i})

    assert storage.last_index == 10

    logging.debug("ABC")
    assert await storage.rollback(4) is True
    logging.debug("DEF")

    assert storage.last_index == 4

    await storage.close()


async def test_rollback_safeguard(tmp_path):
    storage = Storage(tmp_path / "journal")
    await storage.open()

    for i in range(10):
        await storage.commit(10, {"tid": i})

    storage.snapshot_index = 5

    assert await storage.rollback(4) is False

    await storage.close()


async def test_step_rollback(tmp_path):
    storage = Storage(tmp_path / "journal")
    await storage.open()

    machine = Machine("node1")
    machine.term = 2
    machine.log.append((1, {"type": "consensus"}))
    machine.log.append((2, {}))

    await storage.step(machine)

    machine.log.truncate(1)
    machine.log.append((3, {}))
    assert machine.log[2] == (3, {})

    await storage.step(machine)

    assert storage.log[1] == [3, {}]
