"""Domain layer — pure business logic, no infrastructure imports.

Hexagonal: this layer is dependency-free w.r.t. adapters. The 5-state
saga lifecycle, AGID-rejection invariant, federated ack gate, append-only
history all live here.
"""
