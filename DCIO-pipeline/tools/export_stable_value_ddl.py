"""Export the actual router schemas and unified view for review."""
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from route_stable_value import COMMON_COLUMNS, AUDIT_COLUMNS, TARGETS, unified_view_sql

directory = root / 'sql' / 'stable_value'
directory.mkdir(parents=True, exist_ok=True)
statements = ['-- Downstream-only design. No staging or CIT schema alterations.']
for table, columns in [*[(t, COMMON_COLUMNS) for t in TARGETS], ('plan_stable_value_routing_audit', AUDIT_COLUMNS)]:
    definitions = ',\n'.join(f'  {name} {kind}' for name, kind in columns.items())
    statements.append(f"CREATE TABLE IF NOT EXISTS default.{table} (\n{definitions}\n)\nLOCATION 's3://retirementinsights-silver/tables/default.db/{table}'\nTBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');")
(directory / '01_create_history_and_audit.sql').write_text('\n\n'.join(statements) + '\n', encoding='utf-8')
(directory / '02_create_unified_view.sql').write_text(unified_view_sql('default') + ';\n', encoding='utf-8')
