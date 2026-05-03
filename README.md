# comlink

A Go implementation of the **Consul** fault-tolerant communication substrate
described in:

> Mishra, S., Peterson, L. L., & Schlichting, R. D. (1993). *Consul: a
> communication substrate for fault-tolerant distributed programs.*
> Distributed Systems Engineering, 1(2), 87–103.
> [DOI 10.1088/0967-1846/1/2/004](https://iopscience.iop.org/article/10.1088/0967-1846/1/2/004).

The goal is a reusable, idiomatic Go library that other distributed systems
can be built on top of: replicated state machines, replicated stores,
group-membership-based services.

## Status

**Under construction.** See [`PLAN.md`](PLAN.md) for the multi-session
implementation plan, design decisions, and per-phase exit criteria.

## License

Apache-2.0.
