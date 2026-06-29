import pytest
from harness.types import Task, TaskStatus


@pytest.mark.asyncio
async def test_echo_agent(echo_agent):
    await echo_agent.start()
    task   = Task(task_type="echo", payload={"key": "value"})
    result = await echo_agent.execute(task)
    assert result.status == TaskStatus.SUCCEEDED
    assert result.output == {"key": "value"}
    await echo_agent.stop()


@pytest.mark.asyncio
async def test_transform_upper(transform_agent):
    await transform_agent.start()
    task   = Task(task_type="transform", payload={"text": "hello", "operation": "upper"})
    result = await transform_agent.execute(task)
    assert result.output == "HELLO"
    await transform_agent.stop()


@pytest.mark.asyncio
async def test_transform_reverse(transform_agent):
    await transform_agent.start()
    task   = Task(task_type="transform", payload={"text": "abc", "operation": "reverse"})
    result = await transform_agent.execute(task)
    assert result.output == "cba"
    await transform_agent.stop()


@pytest.mark.asyncio
async def test_transform_unknown_op_raises(transform_agent):
    await transform_agent.start()
    task = Task(task_type="transform", payload={"text": "x", "operation": "explode"})
    with pytest.raises(ValueError):
        await transform_agent.execute(task)
    await transform_agent.stop()


@pytest.mark.asyncio
async def test_filter_by_min_length(filter_agent):
    await filter_agent.start()
    task   = Task(task_type="filter", payload={"items": ["a", "bb", "ccc", "dddd"], "min_length": 3})
    result = await filter_agent.execute(task)
    assert result.output == ["ccc", "dddd"]
    await filter_agent.stop()


@pytest.mark.asyncio
async def test_filter_by_min_value(filter_agent):
    await filter_agent.start()
    task   = Task(task_type="filter", payload={"items": [1, 5, 10, 3, 7], "min_value": 5})
    result = await filter_agent.execute(task)
    assert result.output == [5, 10, 7]
    await filter_agent.stop()
