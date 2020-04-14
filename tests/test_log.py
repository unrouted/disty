import logging

from distribd.log import Log


async def test_set_term(tmp_path):
    with open(tmp_path / "term", "w") as fp:
        fp.write("10")

    log = Log(tmp_path / "journal")
    await log.open()

    # Assert term restored from disk
    assert log.current_term == 10

    # Assert can move term forward
    await log.set_term(20)
    assert log.current_term == 20

    # Assert log can't rewind
    await log.set_term(15)
    assert log.current_term == 20

    # Assert term saved to disk
    with open(tmp_path / "term", "r") as fp:
        assert fp.read() == "20"

    await log.close()


async def test_rollback(tmp_path):
    with open(tmp_path / "term", "w") as fp:
        fp.write("10")

    log = Log(tmp_path / "journal")
    await log.open()

    for i in range(10):
        await log.commit(10, {"tid": i})

    assert log.last_index == 10

    logging.debug("ABC")
    assert await log.rollback(4) is True
    logging.debug("DEF")

    assert log.last_index == 4

    await log.close()


async def test_rollback_safeguard(tmp_path):
    log = Log(tmp_path / "journal")
    await log.open()

    for i in range(10):
        await log.commit(10, {"tid": i})

    log.snapshot_index = 5

    assert await log.rollback(4) is False

    await log.close()
