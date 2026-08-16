"""Capture the wire shape a REAL v2 producer emits for a registered custom type.

Do not guess the envelope — make nautilus write one and read it back.
"""

# ⛔ REDIS PORT SAFETY — READ BEFORE RUNNING
# On the Tatwam Mac mini, port 6379 is `at-redis`: THE LIVE PAPER-TRADING STACK that
# at-app and at-worker depend on. These scripts originally hardcoded 6379 and left real
# residue in it (trader-PROBE-SRC-001:*). One 25s msgbus run can write 114,869 keys.
# Default is now a THROWAWAY port. Start one with:
#     docker run -d --name probe-redis -p 6390:6379 redis:7-alpine
# Override deliberately with REDIS_PORT=... , never casually.
import os as _os
REDIS_PORT = int(_os.environ.get("REDIS_PORT", "6390"))
import json, os, threading, time, redis
from nautilus_trader.common import (Environment, SerializationEncoding,
                                    LoggerConfig, LogLevel)
from nautilus_trader.config import MessageBusConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (TraderId, DataType, register_custom_data_class)
from nautilus_trader.trading import Strategy
import nautilus_trader.infrastructure as I

TYPE = "ProbeCmd"


class ProbeCmd:
    def __init__(self, value="", ts_event=0, ts_init=0):
        self.value = value; self.ts_event = ts_event; self.ts_init = ts_init

    @classmethod
    def type_name_static(cls): return TYPE

    @classmethod
    def decode_record_batch_py(cls, metadata, ipc_bytes): raise NotImplementedError

    @classmethod
    def from_json(cls, data):
        d = json.loads(data if isinstance(data, (str, bytes)) else bytes(data))
        return cls(**{k: d[k] for k in ("value", "ts_event", "ts_init") if k in d})

    def to_json(self):
        return json.dumps({"value": self.value, "ts_event": self.ts_event,
                           "ts_init": self.ts_init}).encode()

    def encode_record_batch_py(self, items): raise NotImplementedError


register_custom_data_class(ProbeCmd)
print("  MARK registration OK", flush=True)


class Pub(Strategy):
    def on_start(self):
        try:
            self.publish_data(DataType(TYPE), ProbeCmd("FROM-REAL-PRODUCER", 1, 1))
            print("  MARK publish_data called", flush=True)
        except Exception as e:
            print(f"  MARK publish_data FAILED {type(e).__name__}: {e}", flush=True)


mbc = MessageBusConfig(streams_prefix="stream", stream_per_topic=True,
                       encoding=SerializationEncoding.JSON, use_instance_id=False,
                       types_filter=None)
node = (LiveNode.builder(name="p", trader_id=TraderId("PROBE-CAP-001"),
                         environment=Environment.LIVE)
        .with_msgbus_config(mbc)
        .with_external_msgbus_factory(I.RedisMessageBusFactory())
        .with_logging(LoggerConfig(LogLevel.DEBUG))
        .build())
node.add_strategy(Pub())


def bail():
    time.sleep(12)
    r = redis.Redis(host="127.0.0.1", port=REDIS_PORT, db=0)
    keys = [k.decode() for k in r.keys("trader-PROBE-CAP-001*")]
    print(f"  MARK keys written: {keys}", flush=True)
    for k in keys:
        for _id, f in r.xrevrange(k, count=1):
            print(f"  MARK SHAPE {k}", flush=True)
            for fk, fv in f.items():
                print(f"      {fk!r} = {fv!r}", flush=True)
    os._exit(0)


threading.Thread(target=bail, daemon=True).start()
node.run()
