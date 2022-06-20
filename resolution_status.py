from enum import Enum

# Current status of a resolution in the resolution queue

class ResolutionStatus(Enum):
    PENDING = "pending",
    ACTIVE = "active",
    QUERYING = "querying"
    BLOCKED = "blocked",
    DONE = "done",
    TIMEOUT = "timeout",