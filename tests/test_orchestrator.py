import asyncio
import pytest
from harness.aggregator  import ResultAggregator
from harness.bus         import MessageBus
from harness.orchestrator import Orchestrator
from harness.registry    import AgentRegistry
from harness.types       import Task, TaskStatus
from agents.echo_agent      import EchoAgent
from agents.transform_agent import TransformAgent


async def _build(concurrency: int = 5):
    bus   = MessageBus()
    reg   = AgentRegistry()
    agg   = ResultAggregator()
    orch  = Orchestrator(reg, bus, agg, concurrency=concurrency)
    return bus, reg, agg, orch


@pytest.mark.asyncio
async def test_echo_task_succeeds():
    bus, reg, agg, orch = await _build()
    agent = EchoAgent("echo-1", bus)
    reg.register(agent)
    await reg.start_all()

    run = asyncio.create_task(orch.run())
    task = Task(task_type="echo", payload={"value": 42})
    await orch.submit(task)

    result = await agg.wait_for(task.task_id, timeout=2.0)
    assert result.status == TaskStatus.SUCCEEDED
    assert result.output == {"value": 42}

    await orch.shutdown()
    run.cancel()
    await reg.stop_all()


@pytest.mark.asyncio
async def test_transform_task():
    bus, reg, agg, orch = await _build()
    agent = TransformAgent("transform-1", bus)
    reg.register(agent)
    await reg.start_all()

    run = asyncio.create_task(orch.run())
    task = Task(task_type="transform", payload={"text": "hello", "operation": "upper"})
    await orch.submit(task)

    result = await agg.wait_for(task.task_id, timeout=2.0)
    assert result.status == TaskStatus.SUCCEEDED
    assert result.output == "HELLO"

    await orch.shutdown()
    run.cancel()
    await reg.stop_all()


@pytest.mark.asyncio
async def test_no_agent_for_type_fails_gracefully():
    bus, reg, agg, orch = await _build()

    run  = asyncio.create_task(orch.run())
    task = Task(task_type="unknown_type", payload=None)
    await orch.submit(task)

    result = await agg.wait_for(task.task_id, timeout=2.0)
    assert result.status == TaskStatus.FAILED
    assert "No healthy agent" in result.error

    await orch.shutdown()
    run.cancel()


@pytest.mark.asyncio
async def test_multiple_tasks_parallel():
    bus, reg, agg, orch = await _build(concurrency=10)
    for i in range(3):
        reg.register(EchoAgent(f"echo-{i}", bus))
    await reg.start_all()

    run   = asyncio.create_task(orch.run())
    tasks = [Task(task_type="echo", payload=i) for i in range(10)]
    for t in tasks:
        await orch.submit(t)

    results = await asyncio.gather(
        *(agg.wait_for(t.task_id, timeout=3.0) for t in tasks)
    )
    assert all(r.status == TaskStatus.SUCCEEDED for r in results)
    assert {r.output for r in results} == set(range(10))

    await orch.shutdown()
    run.cancel()
    await reg.stop_all()
