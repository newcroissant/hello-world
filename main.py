"""
Entry point demonstrating the multi-agent harness in action.
Runs a mixed workload of echo, transform, and filter tasks concurrently.
"""
import asyncio
import logging

from harness import MessageBus, AgentRegistry, ResultAggregator, Orchestrator, Task
from agents  import EchoAgent, TransformAgent, FilterAgent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


async def main() -> None:
    bus        = MessageBus()
    registry   = AgentRegistry()
    aggregator = ResultAggregator()
    orch       = Orchestrator(registry, bus, aggregator, concurrency=20)

    # Register a pool of agents
    for i in range(2):
        registry.register(EchoAgent(f"echo-{i}", bus))
        registry.register(TransformAgent(f"transform-{i}", bus))
    registry.register(FilterAgent("filter-0", bus))

    await registry.start_all()
    run_task = asyncio.create_task(orch.run())

    # Submit a mixed workload
    tasks = [
        Task(task_type="echo",      payload={"hello": "world"},                    priority=1),
        Task(task_type="transform", payload={"text": "multi-agent harness",        "operation": "upper"}, priority=5),
        Task(task_type="transform", payload={"text": "Hello World",                "operation": "title"}, priority=3),
        Task(task_type="filter",    payload={"items": ["cat", "elephant", "ox"],   "min_length": 3},      priority=2),
        Task(task_type="filter",    payload={"items": [1, 42, 7, 100, 3],          "min_value": 10},      priority=4),
        Task(task_type="echo",      payload="high priority ping",                  priority=10),
    ]

    for t in tasks:
        await orch.submit(t)

    # Await all results
    results = await asyncio.gather(
        *(aggregator.wait_for(t.task_id, timeout=10.0) for t in tasks)
    )

    print("\n=== Results ===")
    for r in results:
        status = r.status.name
        print(f"[{status:9s}] agent={r.agent_id:<15s} output={r.output!r}")

    print(f"\nSummary: {aggregator.summary()}")

    health = await registry.health_check_all()
    print("\n=== Agent Health ===")
    for h in health:
        print(f"  {h.agent_id:<18s} status={h.status.name} tasks={h.tasks_done} errors={h.error_rate:.0%}")

    await orch.shutdown()
    run_task.cancel()
    await registry.stop_all()


if __name__ == "__main__":
    asyncio.run(main())
