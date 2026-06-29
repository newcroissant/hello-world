import pytest
from harness.errors   import AgentRegistrationError
from harness.registry import AgentRegistry
from harness.types    import AgentStatus


@pytest.mark.asyncio
async def test_register_and_start(echo_agent, registry):
    registry.register(echo_agent)
    assert echo_agent.agent_id in registry.agent_ids
    assert registry.status(echo_agent.agent_id) == AgentStatus.REGISTERED

    await registry.start_all()
    assert registry.status(echo_agent.agent_id) == AgentStatus.HEALTHY
    await registry.stop_all()


@pytest.mark.asyncio
async def test_agents_for_type(echo_agent, transform_agent, registry):
    registry.register(echo_agent)
    registry.register(transform_agent)
    await registry.start_all()

    echo_agents      = registry.agents_for_type("echo")
    transform_agents = registry.agents_for_type("transform")
    unknown_agents   = registry.agents_for_type("nonexistent")

    assert len(echo_agents) == 1
    assert echo_agents[0].agent_id == "echo-1"
    assert len(transform_agents) == 1
    assert unknown_agents == []

    await registry.stop_all()


def test_register_non_worker_raises(registry):
    with pytest.raises(AgentRegistrationError):
        registry.register("not_a_worker")  # type: ignore


@pytest.mark.asyncio
async def test_health_check_all(echo_agent, registry):
    registry.register(echo_agent)
    await registry.start_all()

    reports = await registry.health_check_all()
    assert len(reports) == 1
    assert reports[0].agent_id == "echo-1"

    await registry.stop_all()
