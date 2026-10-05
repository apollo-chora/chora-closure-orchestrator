"""PII_Closure_Map.yaml loader + types.

Each domain ships a ``config/PII_Closure_Map.yaml`` declaring fields-to-tokenize,
retention rules, and on-creator-closure behaviour. The loader is shared
machinery used by per-domain pseudonymisation subscribers — each domain
owns its file; this loader is the canonical reader.

Federated ownership: Team 3 (Platform) owns the loader; each domain team
owns their own ``PII_Closure_Map.yaml``.
"""

from chora_closure_orchestrator.adapter.pii_map.loader import (
    InvalidPIIMapError,
    PIIClosureMap,
    PIIFieldRule,
    PIITableRules,
    load_pii_map,
    load_pii_map_from_file,
)

__all__ = [
    "InvalidPIIMapError",
    "PIIClosureMap",
    "PIIFieldRule",
    "PIITableRules",
    "load_pii_map",
    "load_pii_map_from_file",
]
