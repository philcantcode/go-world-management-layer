# ADR 0010: Permit arbitrary guest control through scoped target transports

- Status: accepted
- Date: 2026-07-27

## Context

Vulnerability research is exploratory. An agent may need to install an
unexpected debugger, push a newly built binary, replace Frida server, run an
arbitrary shell pipeline, configure a proxy, create ADB forwards, reboot an
emulator, or combine tools that the control plane did not anticipate. A growing
allowlist of semantic operations would block useful work and turn the world
manager into a poor reimplementation of Linux shells and ADB.

At the same time, giving the agent Docker, raw host ADB, host paths, or target
selection would turn convenient guest control into infrastructure authority.
The system also needs to distinguish agent intervention from specimen behavior
without pretending every temporal correlation is causal.

## Decision

Apply the rule **arbitrary authority inside the assigned disposable target; no
authority outside it**.

Each active target run exposes provider-neutral data-plane transports:

- arbitrary direct argv and explicit shell streams for Linux targets;
- bounded push/pull streams using workspace-beneath and target-relative paths;
  and
- an ordinary ADB-compatible endpoint exposing exactly one assigned Android
  serial and arbitrary device-scoped ADB services supported by that device.

There is no per-command semantic allowlist or approval loop. Resource, time,
storage, stream, capture, and target-network limits still apply. Gateways reject
host execution, Docker/runtime operations, arbitrary mounts, raw USB, host ADB-
server control, other serials, other targets, and inactive lease/run scopes.

MCP is the lifecycle, discovery, and evidence-query facade. It may offer a
convenience exec operation, but interactive/high-volume command streams use
`world-target` or an ordinary ADB client and are not forced through MCP.

Every exec, shell, transfer, and ADB request receives a `TargetOperationID`.
The ledger records bounded/redacted command metadata, content hashes, process
identity and ancestry where observable, lifecycle effects, and coverage
changes. Observation bundles distinguish declared specimen, agent control,
agent-installed instrumentation, system behavior, and mixed/unknown origin.
Required observers stay outside the guest when possible. Disrupting an in-guest
observer creates a coverage gap; it does not silently block the command.

## Consequences

- Agents can install Frida and other unforeseen tools without new world API
  releases.
- The security boundary is expressed structurally by lease, target, generation,
  endpoint, namespaces, and credentials rather than by parsing shell text.
- Root inside a rooted Android image or Linux target is intentionally possible
  and is not equivalent to host root.
- Agent interventions may change timing and behavior. Bundles retain those
  effects and coverage changes so analysis can account for them.
- Gateways, transfer services, and operation attribution require adversarial
  protocol, race, resource-exhaustion, and cross-target tests.

## Rejected alternatives

- Add a typed API operation for every research action: incomplete by design and
  slow to evolve.
- Put all command traffic through MCP: poor fit for interactive terminals, ADB,
  and high-volume byte streams.
- Give the agent Docker or the host ADB server: expands target control into host
  and cross-target authority.
- Block commands that may disrupt observation: conflicts with visibility-first
  research and conceals meaningful anti-observation behavior.
