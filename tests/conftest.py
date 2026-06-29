import pytest
import asyncio

from harness.bus        import MessageBus
from harness.registry   import AgentRegistry
from harness.aggregator import ResultAggregator
from agents.echo_agent      import EchoAgent
from agents.transform_agent import TransformAgent
from agents.filter_agent    import FilterAgent


@pytest.fixture
def bus():
    return MessageBus()


@pytest.fixture
def registry():
    return AgentRegistry()


@pytest.fixture
def aggregator():
    return ResultAggregator()


@pytest.fixture
def echo_agent(bus):
    return EchoAgent("echo-1", bus)


@pytest.fixture
def transform_agent(bus):
    return TransformAgent("transform-1", bus)


@pytest.fixture
def filter_agent(bus):
    return FilterAgent("filter-1", bus)
