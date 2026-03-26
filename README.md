# Nonlinear Phonon Calculation Stable Releases

This repository tracks promoted stable snapshots of the `Nonlinear Phonon Calculation` bundle.

Development does not happen here. This repository is the release ledger:

- source development happens in the working repository
- only promoted stable bundle snapshots are committed here
- the current stable pointer is stored in `releases/stable/github_release_bundle_current.json`

## Current Stable

Current promoted version:

- `github_release_bundle_2026-03-26_183955`

Current pointer:

- `releases/stable/github_release_bundle_current.json`

Current stable directory:

- `releases/stable/github_release_bundle_2026-03-26_183955/`

That snapshot contains:

- the uncompressed stable bundle
- the bundle tarball
- build metadata
- source branch and source commit markers
- the operator-facing README set
- the bundled WSe2 example and contract sample

## Layout

```text
releases/
  stable/
    github_release_bundle_current.json
    github_release_bundle_<version>/
```

## Notes

- This repository is updated by promoting a new stable snapshot and committing that promoted result.
- Older incorrect or superseded snapshots are not kept as the active stable here.
- If you want the actual runnable bundle, go into the latest directory under `releases/stable/`.
