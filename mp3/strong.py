from __future__ import annotations

from concurrent.futures import Future
import math
import time
from typing import TYPE_CHECKING

from .config import MAX_VALUE_BYTES
from .coordinator import (
    OperationStatus, OwnerCall, OwnerOutcome, ProtocolResult, RpcCompletion,
    collect_distinct,
)
from .models import ConsistencyMode, Record, Version
from .rpc import (
    RpcOperation, RpcOutcome, RpcOutcomeKind, new_operation_id, new_request,
)

if TYPE_CHECKING:
    from .node import CoordinatorAdmission, NodeRuntime
    from .ring import Node


QUORUM_SIZE = 3
PHASE_RESERVATION_SIZE = 10


class StrongCoordinator:
    def __init__(self, runtime: "NodeRuntime"):
        self.runtime = runtime

    def strong_put(self, key: str, value: bytes, *, deadline: float) -> ProtocolResult:
        fallback_id = new_operation_id()
        if not isinstance(value, bytes) or len(value) > MAX_VALUE_BYTES:
            return self._result(OperationStatus.ERROR, fallback_id,
                                error=f"value must be bytes of size 0..{MAX_VALUE_BYTES}")
        invalid = self._validate_operation(key, deadline, fallback_id)
        if invalid is not None:
            return invalid
        gate_error = self._external_gate(fallback_id)
        if gate_error is not None:
            return gate_error
        try:
            future = self.runtime.submit_coordinator(
                self._run, "put", key, value, deadline=deadline)
        except (RuntimeError, ValueError) as error:
            return self._result(OperationStatus.ERROR, fallback_id, error=str(error))
        if future is None:
            return self._result(OperationStatus.OVERLOADED, fallback_id,
                                error="coordinator capacity unavailable before dispatch")
        try:
            return future.result()
        except Exception as error:
            return self._result(OperationStatus.ERROR, fallback_id, error=str(error))

    def strong_get(self, key: str, *, deadline: float) -> ProtocolResult:
        fallback_id = new_operation_id()
        invalid = self._validate_operation(key, deadline, fallback_id)
        if invalid is not None:
            return invalid
        gate_error = self._external_gate(fallback_id)
        if gate_error is not None:
            return gate_error
        try:
            future = self.runtime.submit_coordinator(
                self._run, "get", key, None, deadline=deadline)
        except (RuntimeError, ValueError) as error:
            return self._result(OperationStatus.ERROR, fallback_id, error=str(error))
        if future is None:
            return self._result(OperationStatus.OVERLOADED, fallback_id,
                                error="coordinator capacity unavailable before dispatch")
        try:
            return future.result()
        except Exception as error:
            return self._result(OperationStatus.ERROR, fallback_id, error=str(error))

    def _validate_operation(self, key: str, deadline: float,
                            operation_id: str) -> ProtocolResult | None:
        if not isinstance(key, str):
            return self._result(OperationStatus.ERROR, operation_id, error="key must be text")
        try:
            key.encode("utf-8", errors="strict")
        except UnicodeError as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            return self._result(OperationStatus.ERROR, operation_id,
                                error="deadline must be a finite monotonic timestamp")
        return None

    def _external_gate(self, operation_id: str) -> ProtocolResult | None:
        if not self.runtime.ready:
            return self._result(OperationStatus.ERROR, operation_id,
                                error="node is not ready")
        activation = self.runtime.protocol_activation
        if activation is None or not activation.enabled_for(self.runtime.manifest):
            return self._result(OperationStatus.ERROR, operation_id,
                                error="SCHEMA2_DISABLED")
        return None

    def _run(self, admission: "CoordinatorAdmission", operation: str,
             key: str, value: bytes | None) -> ProtocolResult:
        operation_id = admission.operation_id
        deadline = admission.deadline
        context = admission.context
        if context.mode != ConsistencyMode.STRONG:
            return self._result(OperationStatus.ERROR, operation_id,
                                error="Strong operation requires Strong mode")

        owners = self.runtime.manifest.owners(key)
        if len(owners) != 5 or len({node.vm_id for node in owners}) != 5:
            return self._result(OperationStatus.ERROR, operation_id,
                                error="manifest did not return five distinct owners")

        try:
            reservation = self.runtime.reserve_attempts(
                PHASE_RESERVATION_SIZE, deadline=deadline, admission=admission)
        except (RuntimeError, ValueError) as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        if reservation is None:
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                error="cannot reserve both Strong phases before dispatch")

        write_phase_started = False
        try:
            # phase 1: ask a majority for the latest version
            query_calls = self._dispatch_phase(
                admission, owners, reservation, RpcOperation.READ_LOCAL, key,
                "strong_query", None, b"")
            query = collect_distinct(
                query_calls, owners=owners, manifest=self.runtime.manifest,
                required=QUORUM_SIZE, operation_id=operation_id,
                phase="strong_query", context=context, deadline=deadline)
            if not query.reached:
                return self._collection_failure(
                    query, "strong_query", operation_id, completion_unknown=False)

            selected = self._maximum_record(query)
            if operation == "get" and selected is None:
                return self._result(
                    OperationStatus.NOT_FOUND, operation_id,
                    owners=tuple(reply.owner_id for reply in query.qualifying))

            if operation == "put":
                try:
                    version = self.runtime.store.allocate_strong_version(
                        context=context,
                        observed_max=None if selected is None else selected.version,
                        deadline=deadline)
                except Exception as error:
                    return self._result(OperationStatus.ERROR, operation_id,
                                        error=f"Strong tag allocation failed: {error}")
                assert value is not None
                record = Record(version, value)
                phase = "strong_write"
            else:
                assert selected is not None
                record = selected
                phase = "strong_readback"

            # phase 2: write to a majority (a get writes back what it read)
            write_phase_started = True
            install_calls = self._dispatch_phase(
                admission, owners, reservation, RpcOperation.INSTALL_IF_NEWER,
                key, phase, record.version, record.value)
            installed = collect_distinct(
                install_calls, owners=owners, manifest=self.runtime.manifest,
                required=QUORUM_SIZE, operation_id=operation_id,
                phase=phase, context=context, deadline=deadline)
            if not installed.reached:
                unknown = self._write_completion_unknown(installed)
                return self._collection_failure(
                    installed, phase, operation_id, completion_unknown=unknown)

            if operation == "put":
                return self._result(
                    OperationStatus.OK, operation_id, version=record.version,
                    owners=tuple(reply.owner_id for reply in installed.qualifying))
            return self._result(
                OperationStatus.OK, operation_id, value=record.value,
                version=record.version,
                owners=tuple(reply.owner_id for reply in installed.qualifying))
        except Exception as error:
            return self._result(
                OperationStatus.ERROR, operation_id,
                completion_unknown=write_phase_started,
                error=f"Strong {operation} failed: {type(error).__name__}: {error}")
        finally:
            reservation.close()

    def _dispatch_phase(self, admission: "CoordinatorAdmission",
                        owners: tuple["Node", ...], reservation,
                        operation: RpcOperation, key: str, phase: str,
                        version: Version | None, value: bytes) -> tuple[OwnerCall, ...]:
        calls = []
        for owner in owners:
            try:
                call = self.runtime.dispatch_owner(
                    owner.vm_id, operation, key, deadline=admission.deadline,
                    reservation=reservation, phase=phase,
                    operation_id=admission.operation_id, version=version,
                    value=value, admission=admission)
            except Exception as error:
                membership = self.runtime.membership_runtime.table.snapshot()
                member = next(row for row in membership if row.vm_id == owner.vm_id)
                request = new_request(
                    manifest=self.runtime.manifest, sender_id=self.runtime.vm_id,
                    sender_generation=admission.sender_generation,
                    target_id=owner.vm_id, epoch=admission.context.epoch,
                    mode=admission.context.mode, op=operation, key=key,
                    version=version, value=value, phase=phase, schema=2,
                    operation_id=admission.operation_id)
                outcome_kind = (RpcOutcomeKind.TIMEOUT if isinstance(error, TimeoutError)
                                else RpcOutcomeKind.ERROR)
                future = Future()
                future.set_result(RpcCompletion(
                    RpcOutcome(outcome_kind, error=str(error)), time.monotonic()))
                call = OwnerCall(owner, request, member.restart_generation, future)
            calls.append(call)
        return tuple(calls)

    @staticmethod
    def _maximum_record(collection) -> Record | None:
        maximum = None
        for reply in collection.qualifying:
            response = reply.response
            if response.status.value == "FOUND":
                record = Record(response.version, response.value)
                if maximum is None or record.version.compare(maximum.version) > 0:
                    maximum = record
        return maximum

    @staticmethod
    def _write_completion_unknown(collection) -> bool:
        return (collection.completion_unknown or bool(collection.continuing)
                or bool(collection.qualifying))

    @staticmethod
    def _collection_failure(collection, phase: str, operation_id: str,
                            *, completion_unknown: bool) -> ProtocolResult:
        if collection.fatal_error is not None:
            status = OperationStatus.ERROR
        elif collection.deadline_expired:
            status = OperationStatus.TIMEOUT
        elif (collection.failures and all(
                failure.outcome == OwnerOutcome.OVERLOADED
                for failure in collection.failures)):
            status = OperationStatus.OVERLOADED
        else:
            status = OperationStatus.ERROR
        details = [f"{failure.owner_id}:{failure.outcome.value}"
                   + (f"({failure.error})" if failure.error else "")
                   for failure in collection.failures]
        message = (f"{phase} obtained {len(collection.qualifying)}/{QUORUM_SIZE} "
                   "qualifying owner responses")
        if collection.fatal_error:
            message += f"; fatal={collection.fatal_error}"
        if details:
            message += "; failures=" + ", ".join(details)
        if collection.continuing:
            message += "; unfinished owner calls=" + ",".join(
                call.node.vm_id for call in collection.continuing)
        return StrongCoordinator._result(
            status, operation_id, completion_unknown=completion_unknown,
            owners=tuple(reply.owner_id for reply in collection.qualifying),
            error=message)

    @staticmethod
    def _result(status: OperationStatus, operation_id: str, *, value: bytes | None = None,
                version: Version | None = None, owners: tuple[str, ...] = (),
                completion_unknown: bool = False,
                error: str | None = None) -> ProtocolResult:
        return ProtocolResult(status, value, version, operation_id,
                              tuple(dict.fromkeys(owners)), completion_unknown,
                              error=error)
