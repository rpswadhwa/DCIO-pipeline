"""Package only the new router files; never include .env or existing edits."""
from pathlib import Path
import zipfile

root = Path(__file__).resolve().parents[1]
out = root / 'outputs' / 'stable_value_router' / 'stable_value_router.zip'
out.parent.mkdir(parents=True, exist_ok=True)
files = ['route_stable_value.py', 'src/stable_value_manager.py',
         'src/stable_value_routing.py', 'src/stable_value_expanded.py',
         'src/holding_reconciliation.py', 'tests/test_stable_value_expanded.py',
         'tests/test_holding_reconciliation.py', 'src/stable_value_legacy_manager_rules.py',
         'tests/test_stable_value_routing.py', 'tests/test_stable_value_writer.py']
with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as bundle:
    bundle.writestr('src/__init__.py', '')
    for relative in files:
        bundle.write(root / relative, relative)
print(out)
