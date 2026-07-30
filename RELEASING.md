# Releasing

Releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Version 0.x minor releases may contain breaking API, CLI, policy, schema, or
on-disk format changes; patch releases should remain compatible within that
minor line. Do not add compatibility shims during pre-v1 iteration—replace a
contract and all consumers together.

## Public module surfaces

```text
github.com/philcantcode/go-world-management-layer/world
github.com/philcantcode/go-world-management-layer/api/world/v1
github.com/philcantcode/go-world-management-layer/policy
github.com/philcantcode/go-world-management-layer/adapters/agentrunner
github.com/philcantcode/go-world-management-layer/adapters/forensicartifacts
```

Public library surface is `world.Open` / `*world.Manager` (and related session
types). Shipped commands live under `cmd/` (`worldctl`, `world-target`,
`world-observe`, `world-capture`, `world-export`, `world-capabilities`,
`world-guest`, `world-idle`, `verify`). There is no remote daemon product
(`worldd` / `world-node` / `world.Dial` are deleted).

## Checklist

1. Update `CHANGELOG.md`: move ready items under a new version heading with
   today's date and leave an empty `## [Unreleased]` section at the top.
2. Confirm README status, install, and capability claims match what this tag
   actually ships.
3. Run the repository verify gate:

   ```sh
   go run ./cmd/verify
   ```

   Equivalently: `make verify`. For a faster local loop before the full gate:

   ```sh
   go mod verify
   go vet ./...
   go test ./... -count=1
   go test -race ./... -count=1
   ```

4. Commit the release preparation to `main` and wait for CI to pass.
5. Create and push an annotated tag:

   ```sh
   git tag -a v0.4.0 -m "v0.4.0"
   git push origin v0.4.0
   ```

The release workflow reruns tests and creates the GitHub release with generated
notes. If it fails before creating the release, fix the problem and rerun the
workflow; do not move a published tag.

## Consumers

Document the exact tag consumers should pin (`@v0.4.0`). Prefer immutable
digests for deployment profiles, images, and policies over floating `latest`.
