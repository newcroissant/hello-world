from harness.types import Task
from harness.worker import BaseWorker


class FilterAgent(BaseWorker):
    """
    Filters a list of items based on a predicate.

    Expected payload shape:
        {"items": list, "min_length": int}  (for string items)
      or
        {"items": list, "min_value": int | float}  (for numeric items)
    """

    task_types = frozenset({"filter"})

    async def _run(self, task: Task) -> list:
        payload = task.payload
        items   = payload.get("items", [])

        if "min_length" in payload:
            threshold = payload["min_length"]
            return [i for i in items if len(str(i)) >= threshold]

        if "min_value" in payload:
            threshold = payload["min_value"]
            return [i for i in items if i >= threshold]

        return items
