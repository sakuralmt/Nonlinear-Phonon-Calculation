from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / 'inputs'
RAW_SCF_TEMPLATE = INPUT_DIR / 'scf.inp'
STRUCTURE_META_JSON = INPUT_DIR / 'structure_meta.json'
PSEUDO_DIR = INPUT_DIR / 'pseudos'
REQUESTS_JSON = INPUT_DIR / 'requested_pairs.json'
RUN_ROOT = ROOT / 'qe_phonon_pes_run'
RELAX_ROOT = RUN_ROOT / 'relax'
PARAM_TUNING_ROOT = RUN_ROOT / 'param_tuning'
PHONON_TUNING_ROOT = RUN_ROOT / 'phonon_tuning'
PES_TUNING_ROOT = RUN_ROOT / 'pes_tuning'
FRONTEND_ROOT = RUN_ROOT / 'phonon_frontend'
MATDYN_ROOT = RUN_ROOT / 'matdyn'
MODE_CATALOG_ROOT = RUN_ROOT / 'mode_catalog'
PES_ROOT = RUN_ROOT / 'pes_jobs'
PAIR_RESULTS_ROOT = RUN_ROOT / 'pair_results'
RESULTS_ROOT = RUN_ROOT / 'results'
OPTIMIZED_SCF_TEMPLATE = RELAX_ROOT / 'optimized_structure.scf.inp'
SCF_TEMPLATE = RAW_SCF_TEMPLATE
SELECTED_PROFILES_JSON = RESULTS_ROOT / 'selected_profiles.json'

# Generic default: new user structures should still go through QE vc-relax.
# A bundle may opt out for a specific checked-in input via inputs/structure_meta.json.
DEFAULT_INPUT_STRUCTURE_ALREADY_RELAXED = False

PHONON_Q_GRID = (6, 6, 1)
PRIMITIVE_K_MESH = (8, 8, 1)
PES_SCF_PRESET_NAME = 'pes_balanced'
RELAX_STRICT_PRESET_NAME = 'relax_strict'
PHONON_STRICT_PRESET_NAME = 'phonon_strict'
PHONON_BALANCED_PRESET_NAME = 'phonon_balanced'
PES_STRICT_PRESET_NAME = 'pes_strict'
PES_BALANCED_PRESET_NAME = 'pes_balanced'
PES_FAST_PRESET_NAME = 'pes_fast'
PHONON_ACTIVE_PROFILE_LEVEL = 'balanced'
PES_ACTIVE_PROFILE_LEVEL = 'balanced'

RELAX_JOB_PREFIX = 'qerelax'
RELAX_NODES = 1
RELAX_NTASKS = 24
RELAX_PARTITION = 'long'
RELAX_QOS = None
RELAX_TIME = '24:00:00'

PHONON_JOB_PREFIX = 'qeph'
PW_NODES = 1
PH_NODES = 4
Q2R_NODES = 1
MATDYN_NODES = 1
PW_MPI_TASKS = 24
PH_MPI_TASKS = 24
Q2R_MPI_TASKS = 1
MATDYN_MPI_TASKS = 24
PHONON_PARTITION = 'long'
PHONON_QOS = None
PHONON_TIME = '3-00:00:00'

PES_JOB_PREFIX = 'qepes'
PES_NODES = 1
PES_NTASKS = 24
PES_PARTITION = 'long'
PES_QOS = None
PES_TIME = '3-00:00:00'

MODULE_LINES = [
    'module load parallel_studio/2019.0.045 >/dev/null 2>&1 || true',
    'module load intelmpi/2019.0.045 >/dev/null 2>&1 || true',
    'set +u',
    'source /opt/intel/oneapi/setvars.sh >/dev/null 2>&1 || true',
    'set -u',
]

PW_COMMAND = 'mpirun -np {ntasks} pw.x'
PH_COMMAND = 'mpirun -np {ntasks} ph.x'
Q2R_COMMAND = 'mpirun -np {ntasks} q2r.x'
MATDYN_COMMAND = 'mpirun -np {ntasks} matdyn.x'

FILDYN_PREFIX = 'qeph.dyn'
FORCE_CONSTANT_FILE = 'qeph.fc'
MATDYN_FREQ_FILE = 'qeph.freq'
MATDYN_EIG_FILE = 'qeph.eig'

A1_VALS = [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0]
A2_VALS = [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0]

MAX_RUNNING_PES_JOBS = 8
MAX_RUNNING_AUTOTUNE_JOBS = 20
POLL_SECONDS = 20
WAIT_FOR_COMPLETION = True
COLLECT_AT_END = True
RESET_IF_RUN_ROOT_EXISTS = False

# Lightweight vc-relax-based autotune:
# we only run a few low/mid/high points per axis and use the strict vc-relax
# result as the high-accuracy reference.
PHONON_TUNING_AXES = {
    'ecut': [
        {'label': 'ecut60', 'overrides': {'ecutwfc': 60, 'ecutrho': 600}},
        {'label': 'ecut80', 'overrides': {'ecutwfc': 80, 'ecutrho': 800}},
        {'label': 'ecut100', 'overrides': {'ecutwfc': 100, 'ecutrho': 1000}},
        {'label': 'ecut120', 'overrides': {'ecutwfc': 120, 'ecutrho': 1200}},
    ],
    'primitive_k_mesh': [
        {'label': 'k6', 'primitive_k_mesh': [6, 6, 1]},
        {'label': 'k8', 'primitive_k_mesh': [8, 8, 1]},
        {'label': 'k10', 'primitive_k_mesh': [10, 10, 1]},
        {'label': 'k12', 'primitive_k_mesh': [12, 12, 1]},
    ],
    'conv_thr': [
        {'label': 'conv1e8', 'overrides': {'conv_thr': '1.0d-8'}},
        {'label': 'conv1e9', 'overrides': {'conv_thr': '1.0d-9'}},
        {'label': 'conv1e10', 'overrides': {'conv_thr': '1.0d-10'}},
        {'label': 'conv1e11', 'overrides': {'conv_thr': '1.0d-11'}},
    ],
    'degauss': [
        {'label': 'degauss1e7', 'overrides': {'degauss': '1.0d-7'}},
        {'label': 'degauss1e8', 'overrides': {'degauss': '1.0d-8'}},
        {'label': 'degauss1e9', 'overrides': {'degauss': '1.0d-9'}},
        {'label': 'degauss1e10', 'overrides': {'degauss': '1.0d-10'}},
    ],
}

PES_TUNING_AXES = {
    'ecut': [
        {'label': 'ecut50', 'overrides': {'ecutwfc': 50, 'ecutrho': 500}},
        {'label': 'ecut65', 'overrides': {'ecutwfc': 65, 'ecutrho': 650}},
        {'label': 'ecut80', 'overrides': {'ecutwfc': 80, 'ecutrho': 800}},
        {'label': 'ecut100', 'overrides': {'ecutwfc': 100, 'ecutrho': 1000}},
    ],
    'primitive_k_mesh': [
        {'label': 'k4', 'primitive_k_mesh': [4, 4, 1]},
        {'label': 'k6', 'primitive_k_mesh': [6, 6, 1]},
        {'label': 'k8', 'primitive_k_mesh': [8, 8, 1]},
        {'label': 'k12', 'primitive_k_mesh': [12, 12, 1]},
    ],
    'conv_thr': [
        {'label': 'conv1e7', 'overrides': {'conv_thr': '1.0d-7'}},
        {'label': 'conv1e8', 'overrides': {'conv_thr': '1.0d-8'}},
        {'label': 'conv1e9', 'overrides': {'conv_thr': '1.0d-9'}},
        {'label': 'conv1e10', 'overrides': {'conv_thr': '1.0d-10'}},
    ],
    'degauss': [
        {'label': 'degauss1e7', 'overrides': {'degauss': '1.0d-7'}},
        {'label': 'degauss1e8', 'overrides': {'degauss': '1.0d-8'}},
        {'label': 'degauss1e9', 'overrides': {'degauss': '1.0d-9'}},
        {'label': 'degauss1e10', 'overrides': {'degauss': '1.0d-10'}},
    ],
}

COMMON_TUNING_BASE_OVERRIDES = {
    'calculation': 'vc-relax',
    'include_ions': True,
    'include_cell': True,
    'tprnfor': True,
    'tstress': True,
    'ion_dynamics': 'bfgs',
    'cell_dynamics': 'bfgs',
    'press_conv_thr': '0.1',
    'forc_conv_thr': '1.0d-5',
    'ecutwfc': 80,
    'ecutrho': 800,
    'conv_thr': '1.0d-10',
    'degauss': '1.0d-10',
    'primitive_k_mesh': [12, 12, 1],
    'electron_maxstep': 10000,
    'mixing_beta': '0.3d0',
}

COMMON_TUNING_AXES = {
    'ecut': [
        {'label': 'ecut50', 'overrides': {'ecutwfc': 50, 'ecutrho': 500}},
        {'label': 'ecut60', 'overrides': {'ecutwfc': 60, 'ecutrho': 600}},
        {'label': 'ecut65', 'overrides': {'ecutwfc': 65, 'ecutrho': 650}},
        {'label': 'ecut80', 'overrides': {'ecutwfc': 80, 'ecutrho': 800}},
        {'label': 'ecut100', 'overrides': {'ecutwfc': 100, 'ecutrho': 1000}},
        {'label': 'ecut120', 'overrides': {'ecutwfc': 120, 'ecutrho': 1200}},
    ],
    'primitive_k_mesh': [
        {'label': 'k4', 'primitive_k_mesh': [4, 4, 1]},
        {'label': 'k6', 'primitive_k_mesh': [6, 6, 1]},
        {'label': 'k8', 'primitive_k_mesh': [8, 8, 1]},
        {'label': 'k10', 'primitive_k_mesh': [10, 10, 1]},
        {'label': 'k12', 'primitive_k_mesh': [12, 12, 1]},
    ],
    'conv_thr': [
        {'label': 'conv1e7', 'overrides': {'conv_thr': '1.0d-7'}},
        {'label': 'conv1e8', 'overrides': {'conv_thr': '1.0d-8'}},
        {'label': 'conv1e9', 'overrides': {'conv_thr': '1.0d-9'}},
        {'label': 'conv1e10', 'overrides': {'conv_thr': '1.0d-10'}},
        {'label': 'conv1e11', 'overrides': {'conv_thr': '1.0d-11'}},
        {'label': 'conv1e12', 'overrides': {'conv_thr': '1.0d-12'}},
    ],
    'degauss': [
        {'label': 'degauss1e7', 'overrides': {'degauss': '1.0d-7'}},
        {'label': 'degauss1e8', 'overrides': {'degauss': '1.0d-8'}},
        {'label': 'degauss1e9', 'overrides': {'degauss': '1.0d-9'}},
        {'label': 'degauss1e10', 'overrides': {'degauss': '1.0d-10'}},
    ],
}

PHONON_TUNING_BASE_OVERRIDES = {
    'calculation': 'vc-relax',
    'include_ions': True,
    'include_cell': True,
    'tprnfor': True,
    'tstress': True,
    'ion_dynamics': 'bfgs',
    'cell_dynamics': 'bfgs',
    'press_conv_thr': '0.1',
    'forc_conv_thr': '1.0d-5',
    'ecutwfc': 80,
    'ecutrho': 800,
    'conv_thr': '1.0d-10',
    'degauss': '1.0d-10',
    'primitive_k_mesh': [12, 12, 1],
    'electron_maxstep': 10000,
    'mixing_beta': '0.3d0',
}

PES_TUNING_BASE_OVERRIDES = {
    'calculation': 'vc-relax',
    'include_ions': True,
    'include_cell': True,
    'tprnfor': True,
    'tstress': True,
    'ion_dynamics': 'bfgs',
    'cell_dynamics': 'bfgs',
    'press_conv_thr': '0.1',
    'forc_conv_thr': '1.0d-5',
    'ecutwfc': 80,
    'ecutrho': 800,
    'conv_thr': '1.0d-10',
    'degauss': '1.0d-10',
    'primitive_k_mesh': [12, 12, 1],
    'electron_maxstep': 400,
    'mixing_beta': '0.4d0',
}

PHONON_BALANCED_THRESHOLDS = {
    'energy_abs_diff_mev': 2.0,
    'max_position_delta_A': 0.005,
    'max_cell_delta_A': 0.005,
    'max_atomic_force_ry_bohr': 1.0e-3,
}
PHONON_BALANCED_RELAXED_SCALE = 2.0

PES_BALANCED_THRESHOLDS = {
    'energy_abs_diff_mev': 5.0,
    'max_position_delta_A': 0.015,
    'max_cell_delta_A': 0.015,
}
PES_BALANCED_RELAXED_SCALE = 2.0

PES_FAST_THRESHOLDS = {
    'energy_abs_diff_mev': 10.0,
    'max_position_delta_A': 0.03,
    'max_cell_delta_A': 0.03,
}
PES_FAST_RELAXED_SCALE = 1.5
