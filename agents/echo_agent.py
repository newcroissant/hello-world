from harness.types import Task
from harness.worker import BaseWorker


class EchoAgent(BaseWorker):
    """Returns the payload unchanged. Useful as a smoke-test agent."""

    task_types = frozenset({"echo"})

    async def _run(self, task: Task) -> object:
        return task.payload
