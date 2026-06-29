import asyncio
import pytest
from harness.aggregator import ResultAggregator
from harness.types      import TaskResult, TaskStatus


def _result(task_id: str, status: TaskStatus = TaskStatus.SUCCEEDED) -> TaskResult:
    return TaskResult(task_id=task_id, agent_id="agent-1", status=status, output="ok")


@pytest.mark.asyncio
async def test_collect_and_flush():
    agg = ResultAggregator()
    await agg.collect(_result("t1"))
    await agg.collect(_result("t2"))

    results = await agg.flush()
    assert len(results) == 2
    assert await agg.flush() == []


@pytest.mark.asyncio
async def test_wait_for_already_collected():
    agg = ResultAggregator()
    r   = _result("t1")
    await agg.collect(r)

    found = await agg.wait_for("t1", timeout=1.0)
    assert found.task_id == "t1"


@pytest.mark.asyncio
async def test_wait_for_future_result():
    agg = ResultAggregator()

    async def delayed_collect():
        await asyncio.sleep(0.05)
        await agg.collect(_result("t1"))

    asyncio.create_task(delayed_collect())
    found = await agg.wait_for("t1", timeout=1.0)
    assert found.task_id == "t1"


@pytest.mark.asyncio
async def test_wait_for_timeout():
    agg = ResultAggregator()
    with pytest.raises(asyncio.TimeoutError):
        await agg.wait_for("nonexistent", timeout=0.05)


@pytest.mark.asyncio
async def test_summary():
    agg = ResultAggregator()
    await agg.collect(_result("t1", TaskStatus.SUCCEEDED))
    await agg.collect(_result("t2", TaskStatus.FAILED))
    await agg.collect(_result("t3", TaskStatus.SUCCEEDED))

    s = agg.summary()
    assert s["SUCCEEDED"] == 2
    assert s["FAILED"] == 1
