from enum import Enum

# Final response code of a resolution in the resolution queue

class ResolutionResponseCode(Enum):
    # Resolution was successful
    SUCCESS = "success",
    # Resolution lead to hazard and that branch cannot be followed
    ERROR = "error",
    # Resolution had a misconfiguration or other non-breaking error
    WARNING = 'warning'
    # Resolution was not attempted to break a cycle
    LOOP_DETECTED = 'loop_detected'