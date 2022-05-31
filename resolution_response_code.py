from enum import Enum

class ResolutionResponseCode(Enum):
    # Resolution was successful
    SUCCESS = "success",
    # Resolution lead to hazard and that branch cannot be followed
    ERROR = "error",
    # Resolution had a misconfiguration or other non-breaking error
    WARNING = 'warning'