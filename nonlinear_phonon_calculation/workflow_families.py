from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkflowFamilySpec:
    name: str
    description: str
    stage1_screen_grid_n: int
    phonon_q_grid: tuple[int, int, int]
    default_convergence_branch: str


_FAMILIES: dict[str, WorkflowFamilySpec] = {
    "tmd_monolayer_hex": WorkflowFamilySpec(
        name="tmd_monolayer_hex",
        description="Hexagonal TMDS monolayer workflow with Gamma + q + (-q) mode-pair screening.",
        stage1_screen_grid_n=6,
        phonon_q_grid=(6, 6, 1),
        default_convergence_branch="all",
    ),
}


def supported_workflow_families() -> tuple[str, ...]:
    return tuple(sorted(_FAMILIES))


def resolve_workflow_family(name: str) -> WorkflowFamilySpec:
    key = str(name).strip()
    if key not in _FAMILIES:
        raise KeyError(f"Unsupported workflow family: {key}")
    return _FAMILIES[key]
