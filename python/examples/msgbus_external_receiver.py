"""END-TO-END: register a custom data class, then see if a handler ACTUALLY FIRES.

Every prior probe measured the ABSENCE of the next obstacle. This one measures
ARRIVAL. Distinct type name 'ProbeCmd' (not 'Signal') to avoid colliding with a
builtin that may already be registered.
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
import os, json, threading, time
from nautilus_trader.common import (Environment, SerializationEncoding,
                                    LoggerConfig, LogLevel)
from nautilus_trader.config import MessageBusConfig, LiveDataEngineConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (TraderId, ClientId, DataType,
                                   register_custom_data_class)
from nautilus_trader.trading import Strategy

TYPE = "ProbeCmd"
SRC_KEY = f"trader-PROBE-SRC-001:stream:data.{TYPE}"
EXT = ClientId("EXTERNAL")
HITS = []


class ProbeCmd:
    """Payload class carrying BOTH the arrow method (never called on this path)
    and from_json (what delivery actually uses)."""

    def __init__(self, value="", ts_event=0, ts_init=0):
        self.value = value
        self.ts_event = ts_event
        self.ts_init = ts_init

    @classmethod
    def type_name_static(cls):
        return TYPE

    @classmethod
    def decode_record_batch_py(cls, metadata, ipc_bytes):
        # Precondition of the REGISTRAR, not of delivery. If this ever fires,
        # the inbound path is NOT json-only and that is itself the finding.
        print("  MARK !! decode_record_batch_py CALLED — inbound is NOT json-only",
              flush=True)
        raise NotImplementedError

    @classmethod
    def from_json(cls, data):
        # MEASURED: nautilus hands this an ALREADY-PARSED dict, not str/bytes.
        print(f"  MARK from_json CALLED type={type(data).__name__} payload={data}",
              flush=True)
        d = data if isinstance(data, dict) else json.loads(data)
        return cls(value=d.get("value", ""), ts_event=d.get("ts_event", 0),
                   ts_init=d.get("ts_init", 0))

    def to_json(self):
        # MEASURED: must return str, NOT bytes.
        return json.dumps({"value": self.value, "ts_event": self.ts_event,
                           "ts_init": self.ts_init})

    def encode_record_batch_py(self, items):
        raise NotImplementedError

    def __repr__(self):
        return f"ProbeCmd(value={self.value!r})"


# ---- CONTROL: registration must SUCCEED before anything else is meaningful ----
try:
    register_custom_data_class(ProbeCmd)
    print("  MARK CONTROL registration OK", flush=True)
except Exception as e:
    print(f"  MARK CONTROL registration FAILED: {type(e).__name__}: {e}", flush=True)
    os._exit(9)


class Probe(Strategy):
    def on_start(self):
        self.subscribe_data(DataType(TYPE), client_id=EXT)
        print(f"  MARK subscribe_data(DataType('{TYPE}'), client_id=EXTERNAL)",
              flush=True)

    def on_data(self, data):
        HITS.append(("on_data", data))
        print(f"  MARK *** HANDLER FIRED on_data={data!r}", flush=True)

    def on_signal(self, s):
        HITS.append(("on_signal", s))
        print(f"  MARK *** HANDLER FIRED on_signal={s!r}", flush=True)


mbc = MessageBusConfig(external_streams=[SRC_KEY], streams_prefix="stream",
                       stream_per_topic=True, encoding=SerializationEncoding.JSON,
                       use_instance_id=False)
node = (LiveNode.builder(name="p", trader_id=TraderId("PROBE-DST-001"),
                         environment=Environment.LIVE)
        .with_msgbus_config(mbc)
        .with_data_engine_config(LiveDataEngineConfig(external_clients=[EXT]))
        .with_external_msgbus_factory(__import__(
            "nautilus_trader.infrastructure", fromlist=["x"]).RedisMessageBusFactory(
                __import__("nautilus_trader.infrastructure", fromlist=["x"])
                .RedisMessageBusConfig(host="127.0.0.1", port=REDIS_PORT)))
        .with_logging(LoggerConfig(LogLevel.DEBUG))
        .build())
node.add_strategy(Probe())


def bail():
    time.sleep(30)
    print(f"  MARK FINAL hits={len(HITS)}", flush=True)
    os._exit(0)


threading.Thread(target=bail, daemon=True).start()
node.run()
