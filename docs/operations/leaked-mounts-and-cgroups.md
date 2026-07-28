# Leaked mounts and cgroups

Use when teardown leaves a bind mount or cgroup below configured world roots,
or in an external composition that activates the repository's OverlayFS
helpers. The shipped daemon workspace mode is directory copy and does not mount
OverlayFS.

## Procedure

1. Stop admission and quarantine the owning lease or node. Record `findmnt`
   output, relevant `/proc/*/mountinfo`, the cgroup path, `cgroup.events`,
   `cgroup.procs`, controller values, and Docker inspection before mutation.
2. Prove ownership from the configured root plus durable lease/resource IDs and
   Docker `world.*` labels. If ownership is uncertain, stop here and escalate.
3. Close target transports and collectors, then stop the owning target or agent
   container through its driver. Finalize affected execs/runs and evidence
   first when possible.
4. For a mount, identify users of the exact mount, terminate only proven owned
   processes, and unmount leaf-first with an ordinary unmount. Use a lazy or
   forced unmount only under node quarantine after recording why ordinary
   teardown failed.
5. Verify that no mount entry refers to the merged, upper, work, target, or run
   path. Only then remove generation directories through the owning cleanup
   path.
6. For a cgroup, move or terminate only proven owned processes. Wait for
   `populated 0`, then remove empty leaf cgroups before their parents. Never
   recursively kill the configured cgroup root.
7. Run a second inventory and a canary lifecycle. Reopen admission only when no
   path is mounted, no owned process remains, and resource accounting returned
   to baseline.

The Linux helper packages implement validated OverlayFS and cgroup-v2
operations, while the shipped Docker composition owns containers through exact
labels and runtime inspection. There is no safe generic command that can infer
ownership of an arbitrary host path. Deployment service management and
privileged cleanup must preserve this proof-before-action boundary.
