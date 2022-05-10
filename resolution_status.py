from enum import Enum

class ResolutionStatus(Enum):
    PENDING = "pending",
    ACTIVE = "active",
    QUERYING = "querying"
    BLOCKED = "blocked",
    DONE = "done",