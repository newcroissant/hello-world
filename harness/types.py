from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any


class TaskStatus(Enum):
    PENDING   = auto()
    RUNNING   = auto()
    SUCCEEDED = auto()
    FAILED    = auto()
    RETRYING  = auto()


class AgentStatus(Enum):
    REGISTERED = auto()
    STARTING   = auto()
    HEALTHY    = auto()
    DEGRADED   = auto()
    STOPPED    = auto()


@dataclass
class Task:
    task_type:   str
    payload:     Any
    priority:    int            = 0
    task_id:     str            = field(default_factory=lambda: str(uuid.uuid4()))
    retries:     int            = 0
    max_retries: int            = 3
    created_at:  datetime       = field(default_factory=datetime.utcnow)
    deadline:    datetime | None = None


@dataclass
class TaskResult:
    task_id:      str
    agent_id:     str
    status:       TaskStatus
    output:       Any
    error:        str | None   = None
    duration_s:   float        = 0.0
    completed_at: datetime     = field(default_factory=datetime.utcnow)


@dataclass
class Message:
    """Envelope for inter-agent communication on the message bus."""
    sender:    str
    recipient: str      # agent_id or "__all__" for broadcast
    topic:     str
    body:      Any
    msg_id:    str      = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class HealthReport:
    agent_id:    str
    status:      AgentStatus
    queue_depth: int   = 0
    tasks_done:  int   = 0
    error_rate:  float = 0.0
    checked_at:  datetime = field(default_factory=datetime.utcnow)
