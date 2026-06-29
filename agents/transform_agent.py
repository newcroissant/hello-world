from harness.types import Message, Task
from harness.worker import BaseWorker


class TransformAgent(BaseWorker):
    """
    Applies a text transformation to the incoming payload.

    Expected payload shape:
        {"text": str, "operation": "upper" | "lower" | "reverse" | "title"}

    Responds to bus messages with topic "set_default_op" to update the default operation.
    """

    task_types = frozenset({"transform"})

    def __init__(self, agent_id: str, bus, default_op: str = "upper") -> None:
        super().__init__(agent_id, bus)
        self._default_op = default_op

    async def _run(self, task: Task) -> str:
        payload = task.payload
        text    = payload.get("text", "")
        op      = payload.get("operation", self._default_op)

        match op:
            case "upper":   return text.upper()
            case "lower":   return text.lower()
            case "reverse": return text[::-1]
            case "title":   return text.title()
            case _:
                raise ValueError(f"Unknown operation: {op!r}")

    async def on_message(self, msg: Message) -> None:
        if msg.topic == "set_default_op":
            self._default_op = str(msg.body)
        await super().on_message(msg)
