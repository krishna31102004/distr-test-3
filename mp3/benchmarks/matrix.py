"""Authoritative 51-trial matrix for the Oct. 29 MP3 benchmark specification."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib

CLIENT_COUNTS = (1, 2, 4, 8, 10)
TRIALS = 3
MIN_DURATION_SECONDS = 180
E1_CONFIGS = (("strong", None, None), ("eventual-noRR", 1, 1),
              ("eventual-noRR", 2, 2))
E2_CONFIGS = (("eventual-noRR", 1, 1), ("eventual-RR", 1, 1))


@dataclass(frozen=True)
class TrialSpec:
    experiment: str
    mode: str
    read_quorum: int | None
    write_quorum: int | None
    clients: int
    trial: int
    seed: int
    duration_seconds: int = MIN_DURATION_SECONDS

    @property
    def trial_id(self) -> str:
        r = "x" if self.read_quorum is None else str(self.read_quorum)
        w = "x" if self.write_quorum is None else str(self.write_quorum)
        mode = self.mode.replace("-", "_").replace("RR", "rr")
        return f"{self.experiment}-{mode}-r{r}w{w}-c{self.clients}-t{self.trial}"


def seed_for(base_seed: int, experiment: str, trial: int) -> int:
    """Variant-neutral seed: comparable configurations get identical sequences."""
    payload = f"mp3-bench-seed-v1\0{base_seed}\0{experiment}\0{trial}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def full_matrix(base_seed: int = 4252026,
                duration_seconds: int = MIN_DURATION_SECONDS) -> tuple[TrialSpec, ...]:
    if type(duration_seconds) is not int or duration_seconds < MIN_DURATION_SECONDS:
        raise ValueError("full trials must run at least 180 seconds per client")
    rows = []
    for mode, r, w in E1_CONFIGS:
        for clients in CLIENT_COUNTS:
            for trial in range(1, TRIALS + 1):
                rows.append(TrialSpec("exp1", mode, r, w, clients, trial,
                                      seed_for(base_seed, "exp1", trial), duration_seconds))
    for mode, r, w in E2_CONFIGS:
        for trial in range(1, TRIALS + 1):
            rows.append(TrialSpec("exp2", mode, r, w, 10, trial,
                                  seed_for(base_seed, "exp2", trial), duration_seconds))
    return tuple(rows)


def spec_dict(spec: TrialSpec) -> dict:
    return asdict(spec) | {"trial_id": spec.trial_id}
