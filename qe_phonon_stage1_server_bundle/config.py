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
# NOTE:
# - The stage1 frontend currently consumes only the phonon branch from selected_profiles.json.
# - The PES branch is still produced during autotune, but it is consumed downstream by stage3.
# - These PES constants remain here as schema/documentation hints for the autotune output.
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
RELAX_NTASKS = 120
RELAX_PARTITION = 'long'
RELAX_QOS = None
RELAX_TIME = '24:00:00'

PHONON_JOB_PREFIX = 'qeph'
PW_NODES = 1
PH_NODES = 4
Q2R_NODES = 1
MATDYN_NODES = 1
PW_MPI_TASKS = 120
PH_MPI_TASKS = 120
Q2R_MPI_TASKS = 120
MATDYN_MPI_TASKS = 120
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
