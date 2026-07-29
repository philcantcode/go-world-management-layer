# Security policy

## Supported versions

Security fixes are provided for the latest released minor version. Before
version 1.0, users should upgrade to the newest release to receive fixes.

## What this software does and does not do

`go-world-management-layer` is an operational boundary between research agents
and the programs or Android apps they investigate. It:

- authenticates control-plane clients (bearer token or mTLS) and scopes work to
  leases, generations, and owner subjects
- can provision hardened Docker agent workspaces and disposable Linux targets
  from a trusted deployment profile
- treats target command and ADB service bytes as intentionally arbitrary within
  structural lease/run/serial/path scopes
- records failures, observation gaps, and sealed bundles as evidence

It does **not**:

- claim that ordinary Docker containers resist an unknown host-kernel exploit
- enable physical drivers by default; managed `android-emulator` composition is
  opt-in and deployment-profile gated, while daemon-selected Cuttlefish and
  physical-device backends are not shipped
- ship a production collector suite, remote forensic backend, or supported
  host/version matrix

Hostile production workloads require dedicated nodes and completed real-host
escape/security qualification. See the README security boundaries and the
operations runbooks under `docs/operations/`.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository's
[private vulnerability reporting form](https://github.com/philcantcode/go-world-management-layer/security/advisories/new)
and include reproduction steps, affected versions or commits, and the expected
impact.

You should receive an acknowledgement within seven days. Please allow time for
triage and a coordinated fix before disclosing the issue publicly.

## Sensitive data in issues and PRs

Do not attach real bearer tokens, mTLS private keys, production deployment
profiles, host paths that identify private infrastructure, live session or
lease IDs from shared systems, or raw observation bundles that may contain
sensitive target output. Use synthetic fixtures only.
