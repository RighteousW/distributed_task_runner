"""Task definitions and configurations for the distributed task runner."""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict

class TaskPriority(Enum):
    """Priority levels for task execution."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3

@dataclass
class TaskConfig:
    """Configuration for a task to be executed.
    
    Attributes:
        task_id: Unique identifier for the task
        priority: Execution priority level
        payload: Task data and parameters
        max_retries: Maximum retry attempts on failure
        timeout: Execution timeout in seconds
    """
    task_id: str
    priority: TaskPriority
    payload: Dict[str, Any]
    max_retries: int = 3
    timeout: float = 30.0

class Task:
    """Represents a distributable task with state tracking."""
    def __init__(self, task_config: TaskConfig):
        self.task_config = task_config
        self.retry_count = 0
        self.status = "pending"
        self.result = None
        self.assigned_worker = None
        self.created_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize task for network transmission."""
        return {
            "task_id": self.task_config.task_id,
            "priority": self.task_config.priority.value,
            "payload": self.task_config.payload,
            "max_retries": self.task_config.max_retries,
            "timeout": self.task_config.timeout,
            "retry_count": self.retry_count,
            "status": self.status
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Task':
        """Deserialize task from network data."""
        config = TaskConfig(
            task_id=data["task_id"],
            priority=TaskPriority(data["priority"]),
            payload=data["payload"],
            max_retries=data["max_retries"],
            timeout=data["timeout"]
        )
        task = Task(config)
        task.retry_count = data["retry_count"]
        task.status = data["status"]
        return task