"""Check/apply the narrow orchestration patch on the authorized EC2 checkout."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--apply', action='store_true')
parser.add_argument('--bundle-dir', required=True)
args = parser.parse_args()
project = Path('/home/ec2-user/DCIO-pipeline/DCIO-pipeline').resolve()
repo = project.parent
bundle = Path(args.bundle_dir).resolve()
patch = bundle / 'run_pipeline.patch'
text = patch.read_text()
if text.count('--- a/') != 1 or '--- a/src/run_pipeline.py\n' not in text or '+++ b/src/run_pipeline.py\n' not in text:
    raise SystemExit('Patch must modify only src/run_pipeline.py')
for proc in Path('/proc').glob('[0-9]*'):
    if proc.name == str(os.getpid()):
        continue
    try:
        argv = (proc / 'cmdline').read_bytes().decode(errors='replace').split('\0')
    except OSError:
        continue
    if 'src.run_pipeline' in argv or any(a.endswith('/run_pipeline.py') for a in argv):
        raise SystemExit(f'Extraction process {proc.name} is active; deployment must wait')
files = ['route_stable_value.py', 'src/stable_value_pipeline_hook.py',
         'src/stable_value_manager.py', 'src/stable_value_routing.py',
         'src/stable_value_legacy_manager_rules.py', 'src/stable_value_expanded.py',
         'src/holding_reconciliation.py', 'tests/test_stable_value_expanded.py',
         'tests/test_holding_reconciliation.py', 'tests/test_stable_value_pipeline_hook.py',
         'tests/test_stable_value_routing.py', 'tests/test_stable_value_writer.py']
for name in files:
    target = project / name
    if target.exists() and target.read_bytes() != (bundle / name).read_bytes():
        raise SystemExit(f'Refusing to overwrite an existing different file: {target}')
original = project / 'src/run_pipeline.py'
baseline = (bundle / 'run_pipeline_baseline.py').read_bytes()
desired = (bundle / 'src/run_pipeline.py').read_text().replace('\r\n', '\n')
compile(desired, str(original), 'exec')
desired = desired.replace('\n', '\r\n').encode() if b'\r\n' in baseline else desired.encode()
if original.read_bytes() not in (baseline, desired):
    raise SystemExit('Live pipeline changed since inspection; refusing to overwrite it')
print('Exact baseline and new-file checks passed', flush=True)
before = hashlib.sha256(original.read_bytes()).hexdigest()
print('Pipeline before SHA256:', before, flush=True)
if args.apply:
    backup = project / 'stable_value_integration_backups' / before
    backup.mkdir(parents=True, exist_ok=True)
    shutil.copy2(original, backup / 'run_pipeline.py')
    owner = original.stat()
    for name in files:
        target = project / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(bundle / name, target)
            os.chown(target, owner.st_uid, owner.st_gid)
    if original.read_bytes() != baseline:
        raise SystemExit('Pipeline changed during deployment; refusing replacement')
    temporary = original.with_name('.run_pipeline.py.stable_value_install.tmp')
    temporary.write_bytes(desired)
    os.chmod(temporary, owner.st_mode)
    os.chown(temporary, owner.st_uid, owner.st_gid)
    os.replace(temporary, original)
    print('Pipeline after SHA256:', hashlib.sha256(original.read_bytes()).hexdigest(), flush=True)
    print('Preserved backup:', backup / 'run_pipeline.py', flush=True)
