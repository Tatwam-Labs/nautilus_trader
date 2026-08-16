"""Publisher for the end-to-end test. Wire fields must match the REGISTERED type."""

# ⛔ REDIS PORT SAFETY — READ BEFORE RUNNING
# On the Tatwam Mac mini, port 6379 is `at-redis`: THE LIVE PAPER-TRADING STACK that
# at-app and at-worker depend on. These scripts originally hardcoded 6379 and left real
# residue in it (trader-PROBE-SRC-001:*). One 25s msgbus run can write 114,869 keys.
# Default is now a THROWAWAY port. Start one with:
#     docker run -d --name probe-redis -p 6390:6379 redis:7-alpine
# Override deliberately with REDIS_PORT=... , never casually.
import os as _os
REDIS_PORT = int(_os.environ.get("REDIS_PORT", "6390"))
import json, time, redis

TYPE = "ProbeCmd"
SRC_KEY = f"trader-PROBE-SRC-001:stream:data.{TYPE}"
r = redis.Redis(host="127.0.0.1", port=REDIS_PORT, db=0)

# MEASURED envelope shape, captured from CustomData.to_json_bytes().
payload = json.dumps({
    "data_type": {"metadata": {}, "type_name": TYPE},
    "payload": {"value": "HELLO-FROM-OUTSIDE", "ts_event": 1, "ts_init": 1},
    "type": TYPE,
}).encode()
fields = {b"topic": f"data.{TYPE}".encode(), b"type": TYPE.encode(),
          b"payload": payload, b"encoding": b"Json"}
for i in range(10):
    r.xadd(SRC_KEY, fields)
    print(f"  MARK published #{i+1}", flush=True)
    time.sleep(1)
