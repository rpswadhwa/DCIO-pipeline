#!/bin/bash
# Deploys the current local git HEAD's src/config/run scripts to EC2
# (i-0eaee37f64dfe7195) via S3 staging + SSM, then verifies every deployed
# file's MD5 hash matches between local and EC2.
#
# This is the ONLY sanctioned way to update code on EC2. Hand-editing files
# there via SSM one-liners is what let local and EC2 silently diverge for
# weeks (see the 2026-08-12 reconciliation). After a successful run, EC2's
# .deployed_commit marker records exactly which local commit is live, and
# run.sh prints it on every pipeline run.
#
# Usage: ./deploy_to_ec2.sh [--allow-dirty]

set -euo pipefail

INSTANCE_ID="i-0eaee37f64dfe7195"
S3_BUCKET="retirementinsights-bronze"
REGION="us-east-1"
REMOTE_CODE_DIR="/home/ec2-user/DCIO-pipeline/DCIO-pipeline"
SCRATCH="${TMPDIR:-/tmp}"

DEPLOY_PATHS=(
  "DCIO-pipeline/src"
  "DCIO-pipeline/config"
  "DCIO-pipeline/run.sh"
  "DCIO-pipeline/run_classification.py"
  "DCIO-pipeline/cleanup_investment_names.py"
  "DCIO-pipeline/llm_enhance_investments.py"
)

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

PYTHON_BIN="$(command -v python3 || command -v python)"
if [ -z "$PYTHON_BIN" ]; then
  echo "ERROR: no python3/python found on PATH." >&2
  exit 1
fi

if [ -n "$(git status --porcelain)" ] && [ "${1:-}" != "--allow-dirty" ]; then
  echo "ERROR: local working tree is not clean. Commit/stash first, or pass --allow-dirty to deploy anyway." >&2
  git status --short
  exit 1
fi

HEAD_SHA="$(git rev-parse HEAD)"
S3_STAGE="s3://${S3_BUCKET}/dcio_deploy/deploy_${HEAD_SHA}"
LOCAL_TAR="${SCRATCH}/dcio_deploy_${HEAD_SHA}.tar"
LOCAL_HASHES="${SCRATCH}/dcio_deploy_${HEAD_SHA}_local.txt"
REMOTE_HASHES="${SCRATCH}/dcio_deploy_${HEAD_SHA}_remote.txt"
REMOTE_SCRIPT="${SCRATCH}/dcio_deploy_${HEAD_SHA}_remote.sh"

echo "[1/5] Archiving deploy paths at ${HEAD_SHA:0:12} ..."
git archive HEAD -o "$LOCAL_TAR" -- "${DEPLOY_PATHS[@]}"

echo "[2/5] Hashing local archive contents ..."
"$PYTHON_BIN" - "$LOCAL_TAR" > "$LOCAL_HASHES" <<'PYEOF'
import hashlib, sys, tarfile
tf = tarfile.open(sys.argv[1])
rows = []
for m in tf.getmembers():
    if not m.isfile():
        continue
    # strip the leading "DCIO-pipeline/" component so paths line up with EC2's tree
    rel = m.name.split("/", 1)[1] if "/" in m.name else m.name
    h = hashlib.md5(tf.extractfile(m).read()).hexdigest()
    rows.append(f"{h}  {rel}")
for r in sorted(rows, key=lambda x: x.split("  ", 1)[1]):
    print(r)
PYEOF

echo "[3/5] Uploading archive to ${S3_STAGE} ..."
aws s3 cp "$LOCAL_TAR" "${S3_STAGE}/deploy.tar" --region "$REGION" --only-show-errors

cat > "$REMOTE_SCRIPT" <<REMOTEEOF
export HOME=/root
mkdir -p ${REMOTE_CODE_DIR}
aws s3 cp ${S3_STAGE}/deploy.tar /tmp/deploy.tar --region ${REGION}
tar -xf /tmp/deploy.tar --strip-components=1 -C ${REMOTE_CODE_DIR}
echo ${HEAD_SHA} > ${REMOTE_CODE_DIR}/.deployed_commit
date -u +"%Y-%m-%dT%H:%M:%SZ" >> ${REMOTE_CODE_DIR}/.deployed_commit
cd ${REMOTE_CODE_DIR}
find src config -type f -exec md5sum {} \; | sort -k2
for f in run.sh run_classification.py cleanup_investment_names.py llm_enhance_investments.py; do
  [ -f "\$f" ] && md5sum "\$f"
done
python3.11 -c "import compileall,sys; sys.exit(0 if compileall.compile_dir('src', quiet=1) else 1)" && echo COMPILE_OK || echo COMPILE_FAILED
echo DEPLOY_DONE
REMOTEEOF

echo "[4/5] Deploying on EC2 via SSM ..."
B64=$(base64 -w0 "$REMOTE_SCRIPT")
PARAMS_FILE="${SCRATCH}/dcio_deploy_${HEAD_SHA}_params.json"
cat > "$PARAMS_FILE" <<JSON
{"commands": ["export HOME=/root", "echo $B64 | base64 -d > /home/ec2-user/_deploy_${HEAD_SHA}.sh", "bash /home/ec2-user/_deploy_${HEAD_SHA}.sh"]}
JSON

CMD_ID=$(aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name "AWS-RunShellScript" \
  --parameters file://"$PARAMS_FILE" --output text --query "Command.CommandId" --region "$REGION")

for i in $(seq 1 30); do
  STATUS=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
    --query "Status" --output text --region "$REGION" 2>/dev/null || echo "Pending")
  [ "$STATUS" = "Success" ] || [ "$STATUS" = "Failed" ] && break
  sleep 3
done

aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
  --query "[Status,StandardOutputContent,StandardErrorContent]" --output text --region "$REGION" \
  > "${SCRATCH}/dcio_deploy_${HEAD_SHA}_raw.txt" 2>&1
tr -cd '\11\12\15\40-\176' < "${SCRATCH}/dcio_deploy_${HEAD_SHA}_raw.txt" > "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt"

REMOTE_STATUS=$(head -1 "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt")
if [ "$STATUS" != "Success" ]; then
  echo "ERROR: SSM command did not succeed (status: $STATUS)." >&2
  cat "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt" >&2
  exit 1
fi

# Pull just the "<md5>  <path>" lines out of the SSM output into REMOTE_HASHES
grep -E '^[0-9a-f]{32}  ' "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt" | sort -k2 > "$REMOTE_HASHES"

if ! grep -q COMPILE_OK "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt"; then
  echo "ERROR: py_compile failed on EC2 after deploy." >&2
  cat "${SCRATCH}/dcio_deploy_${HEAD_SHA}_clean.txt" >&2
  exit 1
fi

echo "[5/5] Verifying local vs EC2 hashes ..."
# Subset check, not set equality: EC2's src/config dirs carry years of stray
# .bak_* / __pycache__ cruft from pre-deploy-script hand-edits that were never
# part of any deploy and are not this script's concern. We only need every
# file THIS deploy shipped to exist on EC2 with a matching hash.
MISMATCHES="$(awk '
  NR==FNR { remote[$2]=$1; next }
  { if (!($2 in remote)) print "MISSING on EC2: " $2
    else if (remote[$2] != $1) print "HASH MISMATCH: " $2 " (local=" $1 " remote=" remote[$2] ")" }
' "$REMOTE_HASHES" "$LOCAL_HASHES")"

if [ -z "$MISMATCHES" ]; then
  echo "PASS: EC2 has byte-identical copies of all $(wc -l < "$LOCAL_HASHES") deployed file(s) from local commit ${HEAD_SHA:0:12}."
else
  echo "FAIL: deployed files missing or mismatched on EC2:" >&2
  echo -e "$MISMATCHES" >&2
  exit 1
fi
