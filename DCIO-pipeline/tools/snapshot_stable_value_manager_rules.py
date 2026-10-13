"""Extract pure deterministic rules from an inspected ai-cit source snapshot.

Never import the Glue script: its module-level code reads secrets and writes data.
"""
import ast
import hashlib
import pathlib
import sys

FUNCTIONS = {
    'norm', 'sponsor_is_platform', 'sponsor_allows_secondary', 'force_canonical',
    '_match', '_guard_conf', '_extract_result', '_extract_tag', 'apply_rules',
    'contains_token', 'entity_contains_known_manager', 'deterministic_match_debug',
}
CONSTANTS = {
    'PLATFORM_SPONSOR_BLOCKLIST', 'AM_SPONSOR_HINTS', 'SPONSOR_SECONDARY_ALLOWLIST',
    'JPM_SPONSOR_EXTRA_HINTS', 'RULES', 'SHORT_TOKEN_ALLOW', 'STRICT_TOKEN_ALLOW',
}


def snapshot(source, destination):
    source = pathlib.Path(source).read_text(encoding='utf-8-sig')
    nodes = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS:
            nodes[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id in CONSTANTS:
                    ast.literal_eval(node.value)  # require static, data-only rules
                    nodes[target.id] = node
    missing = (FUNCTIONS | CONSTANTS) - nodes.keys()
    if missing:
        raise ValueError(f'Missing deterministic dependencies: {sorted(missing)}')
    text = (
        '"""Pure manager rules snapshotted from the active ai-cit job.\n'
        'No Glue initialization, credentials, LLM calls, or writes.\n'
        f'Source SHA256: {hashlib.sha256(source.encode()).hexdigest()}\n"""\n'
        'from __future__ import annotations\nimport re\nfrom typing import Optional, Tuple, List, Dict\n'
        'Override = Tuple[str, float, str]\nCANON_LOOKUP = {}\nALL_CANONICAL_TOKENS = []\n\n'
    )
    text += '\n\n'.join(ast.unparse(nodes[k]) for k in sorted(CONSTANTS))
    text += '\n\n' + '\n\n'.join(ast.unparse(nodes[k]) for k in sorted(FUNCTIONS)) + '\n'
    pathlib.Path(destination).write_text(text, encoding='utf-8')


if __name__ == '__main__':
    snapshot(sys.argv[1], sys.argv[2])
