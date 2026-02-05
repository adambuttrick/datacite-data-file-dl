"""Exit code constants for CLI."""

from enum import IntEnum


class ExitCode(IntEnum):
    """Exit codes for datacite-dl CLI.

    These follow Unix conventions:
    - 0 for success
    - Non-zero for various failure modes
    """

    SUCCESS = 0
    AUTH_FAILURE = 1
    NETWORK_ERROR = 2
    NOT_FOUND = 3
    PARTIAL_FAILURE = 4
    USER_CANCELLED = 5
