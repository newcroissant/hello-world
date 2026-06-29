class HarnessError(Exception):
    """Base for all harness exceptions."""


class NoAgentAvailableError(HarnessError):
    """No healthy agent is registered for the requested task type."""


class TaskExpiredError(HarnessError):
    """Task deadline was exceeded before it could be dispatched."""


class AgentStartError(HarnessError):
    """An agent failed to start during registry initialisation."""


class AgentRegistrationError(HarnessError):
    """A value passed to register() does not implement the Worker protocol."""
