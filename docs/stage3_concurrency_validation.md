# Stage3 shared Slurm concurrency validation

Validation date: 2026-09-24. All changes and tests are on the independent
`codex/prophet-stage12-v3` worktree. The stable repository was not edited.

The v3 controller registers both materials in one state directory and uses an
account-wide `flock`, so separate state directories cannot run independent
controllers concurrently. Each poll counts this user's queued and running v3
QE jobs and recognized historical Stage3 QE jobs. It subtracts that count from
the global maximum of 30, submits no more than five new points, and alternates
MoS₂ and WSe₂ candidates. A new job gets a 120-second scheduler-visibility
grace period; a failed point is retried at most once. The first scientific
pilot uses only one 81-point pair per material, a five-job cap, and a two-job
submission batch. The controller runs as one `regular` Slurm job. Each QE point
uses 24 MPI ranks and one OpenMP thread per rank.

Local and server Python suites each passed 35 tests. The first real Slurm smoke
controller, job `1019343`, exposed a bug in mixed Prophet/QE queue counting and
exited before submitting any QE job. That bug was corrected and covered by a
new mixed-queue regression test. The second controller, job `1019344`,
completed successfully in one second (`COMPLETED`, exit `0:0`). Its log reports
81/81 synthetic points complete, zero active or pending points, and
`submitted_this_poll=0`. The synthetic inputs are isolated under
`/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/validation/stage3_slurm_smoke/`;
they are not scientific results and were never registered with the scientific
global state directory.

At validation time, four Prophet CPU array tasks ran on four distinct exclusive
56-CPU nodes. No v3 DFT recheck point had been submitted because the common-DFT
Prophet pair rankings were not yet complete. The real 81-point QE pilot must
still measure walltime, SCF convergence, and memory before the 30-job ceiling
is used; the ceiling is a limit, not a throughput target.
