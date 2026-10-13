"""Install reviewed router v2 on the existing EC2 checkout with hash guards."""
import argparse,hashlib,json,os,shutil
from pathlib import Path
parser=argparse.ArgumentParser()
parser.add_argument('--bundle-dir',required=True)
parser.add_argument('--apply',action='store_true')
args=parser.parse_args()
project=Path('/home/ec2-user/DCIO-pipeline/DCIO-pipeline')
bundle=Path(args.bundle_dir)
manifest=json.loads((bundle/'deployment_manifest.json').read_text())
for proc in Path('/proc').glob('[0-9]*'):
    if proc.name==str(os.getpid()): continue
    try: argv=(proc/'cmdline').read_bytes().decode(errors='replace').split('\0')
    except OSError: continue
    if 'src.run_pipeline' in argv or any(a.endswith('/run_pipeline.py') or a.endswith('/route_stable_value.py') or a=='route_stable_value.py' for a in argv):
        raise SystemExit(f'Pipeline or router {proc.name} is active; deployment must wait')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
for name, expected in manifest['baseline'].items():
    if sha(project/name)!=expected: raise SystemExit(f'Live baseline changed: {name}')
for name,expected in manifest['files'].items():
    if sha(bundle/name)!=expected: raise SystemExit(f'Bundle checksum failed: {name}')
    compile((bundle/name).read_text(),name,'exec')
    if name not in manifest['baseline'] and (project/name).exists() and sha(project/name)!=expected:
        raise SystemExit(f'Refusing unrelated existing file: {name}')
print('Baseline and bundle checks passed',flush=True)
if args.apply:
    backup=project/'stable_value_integration_backups'/manifest['deployment_id']
    backup.mkdir(parents=True,exist_ok=False)
    owner=(project/'route_stable_value.py').stat()
    for name in manifest['files']:
        target=project/name
        if target.exists():
            saved=backup/name;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(target,saved)
        target.parent.mkdir(parents=True,exist_ok=True)
        temp=target.with_name('.'+target.name+'.sv2.tmp')
        shutil.copyfile(bundle/name,temp);os.chown(temp,owner.st_uid,owner.st_gid);os.chmod(temp,0o644);os.replace(temp,target)
    (backup/'deployment_manifest.json').write_text(json.dumps(manifest,indent=2))
    for name,expected in manifest['files'].items():
        if sha(project/name)!=expected: raise SystemExit(f'Installed checksum failed: {name}')
    print('Deployed:',json.dumps(manifest['files']),flush=True)
    print('Backup:',backup,flush=True)
