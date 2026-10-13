#!/usr/bin/env bash
# Start a local S3-compatible emulator and create $MIZUCHI_S3_BUCKET.
# Docker Hub no longer serves minio/minio; alpine/minio is a public rebuild.
set -euo pipefail

endpoint="${MIZUCHI_S3_ENDPOINT:-http://127.0.0.1:9000}"
bucket="${MIZUCHI_S3_BUCKET:-mizuchi}"
access="${MIZUCHI_S3_ACCESS_KEY:-minioadmin}"
secret="${MIZUCHI_S3_SECRET_KEY:-minioadmin}"
host="${endpoint#http://}"
host="${host#https://}"
host="${host%/}"

docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER="$access" \
  -e MINIO_ROOT_PASSWORD="$secret" \
  alpine/minio:RELEASE.2025-10-15T17-29-55Z server /home/minio/data

ok=0
for _ in $(seq 1 30); do
  if curl -sf "${endpoint}/minio/health/live"; then
    ok=1
    break
  fi
  sleep 1
done
test "$ok" = 1

python3 - "$host" "$bucket" "$access" "$secret" <<'PY'
import datetime, hashlib, hmac, sys, urllib.error, urllib.request

host, bucket, access, secret = sys.argv[1:5]
now = datetime.datetime.now(datetime.timezone.utc)
amz, date = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
payload = hashlib.sha256(b"").hexdigest()
canonical = (
    f"PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{payload}\n"
    f"x-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
)
scope = f"{date}/us-east-1/s3/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"


def sign(key, msg):
    if isinstance(key, str):
        key = key.encode()
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


k = sign("AWS4" + secret, date)
k = sign(k, "us-east-1")
k = sign(k, "s3")
k = sign(k, "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
req = urllib.request.Request(
    f"http://{host}/{bucket}",
    method="PUT",
    headers={
        "Host": host,
        "x-amz-date": amz,
        "x-amz-content-sha256": payload,
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
            f"Signature={sig}"
        ),
    },
)
try:
    urllib.request.urlopen(req)
except urllib.error.HTTPError as e:
    if e.code != 409:
        raise
PY
