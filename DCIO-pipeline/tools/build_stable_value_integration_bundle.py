"""Package the reviewed integration patch and only its required runtime files."""
import difflib
from pathlib import Path
import subprocess
import zipfile

root = Path(__file__).resolve().parents[1]
out = root / 'outputs' / 'stable_value_router' / 'integration'
out.mkdir(parents=True, exist_ok=True)
base = subprocess.check_output(['git', 'show', 'HEAD:DCIO-pipeline/src/run_pipeline.py'], cwd=root).decode().replace('\r\n', '\n')
changed = (root / 'src/run_pipeline.py').read_text(encoding='utf-8')
patch = ''.join(difflib.unified_diff(base.splitlines(keepends=True), changed.splitlines(keepends=True),
                                  fromfile='a/src/run_pipeline.py', tofile='b/src/run_pipeline.py'))
(out / 'run_pipeline.patch').write_text(patch, encoding='utf-8')
files = ['route_stable_value.py', 'src/stable_value_pipeline_hook.py',
         'src/stable_value_manager.py', 'src/stable_value_routing.py', 'src/stable_value_expanded.py',
         'src/holding_reconciliation.py', 'tests/test_stable_value_expanded.py',
         'tests/test_holding_reconciliation.py',
         'src/stable_value_legacy_manager_rules.py', 'src/run_pipeline.py',
         'tests/test_stable_value_pipeline_hook.py', 'tests/test_stable_value_routing.py',
         'tests/test_stable_value_writer.py']
with zipfile.ZipFile(out / 'stable_value_integration.zip', 'w', zipfile.ZIP_DEFLATED) as bundle:
    for name in files:
        bundle.write(root / name, name)
    bundle.writestr('run_pipeline.patch', patch)
    bundle.write(out / 'ec2_run_pipeline_baseline.py', 'run_pipeline_baseline.py')
print(out / 'stable_value_integration.zip')
