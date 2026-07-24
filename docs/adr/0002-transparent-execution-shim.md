# ADR 0002: Bridge provider process semantics through an opaque shim

- Status: proposed
- Date: 2026-07-24

## Context

`go-agent-runner` starts a selected provider executable directly, probes its
version/help output, writes a prompt, reads machine-readable stdout, drains
stderr, delivers signals, and owns the local process tree. Reimplementing any
provider protocol here would duplicate responsibility and couple this layer to
rapidly changing CLI formats. Running the provider on the host would violate the
core containment requirement.

## Decision

Create a private, lease-specific `worldexec` executable descriptor. The host
passes its path as `agentrunner.Request.Executable`. The shim binds one lease,
generation, and logical provider executable, then transports opaque argv,
working directory, stdin, stdout, stderr, signals, terminal resize, and exit
status to `world-guest` inside the container.

Provider stdout is byte-transparent and never contains management messages.
Management failures are reported out of band; the shim writes only a bounded
terminal diagnostic with an incident ID to stderr and exits non-zero.

Killing or disconnecting the shim triggers bounded cancellation of the remote
exec process group. An unconfirmed cancellation escalates to container teardown
and an incident because the one-container-per-lease runtime is the final kill
domain.

The workspace is mounted at the same absolute path on host and in the container
so provider-generated working-directory arguments remain valid without argv
rewriting.

## Consequences

- Existing provider adapters, probes, schema validation, retry logic, and event
  normalization remain in `go-agent-runner`.
- The agent uses normal tools and does not need a world-specific tool protocol.
- The shim must implement exact flow control, half-close, signal, timeout, and
  disconnect behavior; fake-provider contract tests are mandatory.
- A future runner-native execution backend may replace the shim without
  changing world lease or guest semantics.

## Rejected alternatives

- Parse provider JSON in `worldexec`: duplicates the runner and risks protocol
  corruption.
- Prefix diagnostics onto stdout: breaks provider machine protocols.
- Use ordinary `docker exec` as the whole contract: it does not provide the
  required reconnect, process-group, cancellation, and structured-incident
  semantics.
- Run the provider on the host and only its tools in Docker: an agent or plugin
  could still execute host-side code.
