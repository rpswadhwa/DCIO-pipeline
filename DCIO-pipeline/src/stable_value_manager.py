"""Adapter around existing pure CIT manager rules and the shared canon.json."""
import hashlib
import json
from . import stable_value_legacy_manager_rules as legacy


class ManagerResolver:
    def __init__(self, entries):
        if not isinstance(entries, list) or not entries:
            raise ValueError('Canonical manager dictionary must be a nonempty list')
        self.entries = entries
        self.canon_version = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
        lookup = {}
        for entry in entries:
            canonical = entry.get('canonical')
            if not canonical:
                continue
            for token in [canonical] + entry.get('tokens', []) + entry.get('aliases', []):
                normalized = legacy.norm(token)
                if normalized:
                    lookup[normalized] = canonical
        legacy.CANON_LOOKUP = lookup
        legacy.ALL_CANONICAL_TOKENS = sorted(lookup, key=lambda s: (-len(s), s))

    def resolve(self, entity, sponsor):
        kind, tag, manager, confidence = legacy.deterministic_match_debug(
            entity or '', sponsor or '', self.entries
        )
        if kind == 'NO_MATCH' or not manager:
            return {'normalized_manager': None, 'manager_match_status': 'UNMATCHED',
                    'manager_match_method': tag, 'manager_match_confidence': confidence,
                    'needs_cleaning_flag': True}
        manager = legacy.force_canonical(manager)
        # Insurance issuer/platform is retained as sponsor, not silently called
        # an investment manager by a sponsor-secondary dictionary match.
        if kind == 'SPONSOR_SECONDARY' and legacy.sponsor_is_platform(legacy.norm(sponsor)):
            return {'normalized_manager': None, 'manager_match_status': 'REVIEW',
                    'manager_match_method': 'platform_sponsor_role_unresolved',
                    'manager_match_confidence': None, 'needs_cleaning_flag': True}
        return {'normalized_manager': manager, 'manager_match_status': 'MATCHED',
                'manager_match_method': tag, 'manager_match_confidence': confidence,
                'needs_cleaning_flag': False}
