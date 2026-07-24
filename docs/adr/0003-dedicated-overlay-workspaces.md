# ADR 0003: Use shared immutable input views and dedicated OverlayFS workspaces

- Status: proposed
- Date: 2026-07-24

## Context

Different agent invocations must see different, frozen subsets of forensic
artifacts. Inputs may be large and selections may overlap heavily. Copying a
complete projection for every invocation would waste node storage and make
crash cleanup expensive. Sharing one directory directly would expose entries
outside the authorized selection or let one invocation's metadata choices
affect another.

The forensic repository remains the immutable byte authority. Its public reader
can stream selected objects, but its repository paths and internal content store
must never be visible to an agent. Docker's storage-driver layout is also
internal and is not a stable workspace API.

Agents need a writable derived workspace. OverlayFS supports a common immutable
lower layer and distinct upper layers, but a data write can copy the lower file
into the upper layer. Its whiteouts and opaque directories also mean that a
naive merged-directory walk loses change semantics.

## Decision

The artifact adapter resolves each authorized selection into a canonical
`InputViewManifest`. It contains only the selected logical paths, immutable
object identities, content digests, sizes, modes, and allowed sidecars. The
manifest digest is the `InputViewID`.

`world-node` maintains an expendable content-addressed cache within an explicit
security scope, normally one campaign or tenant. Missing objects are streamed
through the artifact authority's public reader, hashed, staged, and atomically
published. Repository or host source paths are never accepted in a node plan.

The node constructs a read-only view tree for the manifest using reflink clones
on a capability-probed copy-on-write filesystem. Reflinks give each logical
entry independent inode metadata while sharing physical data extents. Exact
manifest digests reuse the same view tree; different overlapping views reuse
the same cached content extents. Hardlink farms are not used because hardlinks
would alias metadata and falsely make equal content at different logical paths
the same file.

Every generation receives new upper and work directories. The cached view is
the read-only OverlayFS lower layer, and only the merged view is mounted into
the container. A generation pins its view and referenced cache entries until
output and incident finalization completes. Unpinned views and content are
removed asynchronously by bounded TTL/LRU garbage collection. Startup
reconciliation rebuilds pins from durable leases, manifests, and live mounts.
The cache is non-authoritative and may always be wiped and rebuilt.

Production policy requires reflink construction and fails admission when the
backing filesystem lacks it. An explicit development-only `allow-copy` mode may
fall back to copying, but the effective policy and physical bytes allocated are
reported so duplication is never silent. Cache sharing across security scopes
is opt-in because cache-hit timing and object presence can themselves leak
information.

The authoritative final change set combines the lower manifest, an
overlay-aware upper scan, the stable merged view, mutation observations, and
hashes from already-open file descriptors. Disagreement fails sealing and
creates an integrity incident.

Exports accept only normalized relative paths and roles. Path opening is
beneath a pre-opened workspace descriptor, does not follow symlinks or magic
links, rejects unsupported file types, applies count/byte quotas, and copies
from the validated descriptor into the host-owned artifact adapter. An agent
never supplies a host destination.

## Consequences

- A unique content digest normally consumes data blocks once per cache scope.
  Each view adds directory/inode metadata, and each generation adds only its
  upper/work state and genuinely copied-up or new data.
- Reset is a new upper/work pair and generation; input views remain immutable.
- A write to a large lower file may copy that whole file into the generation's
  upper layer. Upper-layer quotas therefore measure physical allocation as well
  as logical file size.
- Added, modified, deleted, opaque, and metadata-only changes are explicit.
- Reflink, OverlayFS, and descriptor-safe path handling require Linux-specific
  capability, integration, security, and crash-reconciliation tests.
- View/content cache quota and retention are independent from upper, capture,
  and artifact-retention policies.

## Rejected alternatives

- Materialize every selection for every invocation: simple but duplicates all
  input bytes and leaves large abandoned trees after crashes.
- Use hardlinks for projection entries: saves data blocks but aliases inode
  metadata and cannot faithfully represent independent logical occurrences.
- Bind-mount each selected file: creates excessive mount cardinality and makes
  directory, rename, and metadata semantics difficult to reason about.
- Mount the artifact repository read-only: leaks repository structure and
  creates an unnecessary attack path.
- Inspect Docker's overlay storage directories: unsupported and coupled to the
  daemon's implementation.
- Import every modified file automatically: risks secret/data floods and erases
  the agent or host's explicit retention decision.

Packed immutable images such as EROFS or a composefs-like view builder remain a
future optimization if view cardinality makes reflink tree metadata material.
