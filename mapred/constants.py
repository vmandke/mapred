from enum import Enum

DEFAULT_M = 10
DEFAULT_R = 2
HEARTBEAT_INTERVAL_SECONDS = 1
WORKER_TIMEOUT_SECONDS = 30


class TaskType(Enum):
    MAP = "MAP"
    REDUCE = "REDUCE"


class WorkerStatus(Enum):
    REGISTERED = "REGISTERED"
    INACTIVE = "INACTIVE"
    IDLE = "IDLE"
    BUSY = "BUSY"


class DriverStatus(Enum):
    BUSY = "BUSY"
    IDLE = "IDLE"


class TaskStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
