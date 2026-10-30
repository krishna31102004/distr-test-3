"""Explicit Phase 0 boundary. No storage, network, or mode mutations occur here."""

from .models import Command, OperationResult, Status

DECISIONS = {
    "put": "KD-06/08/09/10/11/12/15",
    "get": "KD-06/08/09/10/11/12/13/15",
    "ls": "KD-02/03",
    "liststore": "KD-09",
    "getfromreplica": "KD-06/08/09/10",
    "list_mem_ids": "KD-02/03/04/05",
    "multiput": "KD-06/08/10/11/12/14",
    "consistency_mode": "KD-16",
}


def execute(command: Command) -> OperationResult:
    """STUB: dispatch after approved decisions and per-command acceptance tests.

    Until then return NOT_IMPLEMENTED for EVERY operation, including mode changes
    and diagnostics. No file is read, created, truncated, or treated as a value.
    See DECISIONS for dependencies; this local guard is not an RPC service API.
    """
    decisions = DECISIONS.get(command.name, "unrecognized command")
    return OperationResult(Status.NOT_IMPLEMENTED,
                           f"{command.name} awaits design review: {decisions}")
