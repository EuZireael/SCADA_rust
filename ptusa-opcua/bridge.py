"""
OPC UA-фасад эмулятора мойки: все каналы станции — узлами OPC UA, данные — от настоящей
прошивки ptusa (эмулятор из папки Moika, tools/ptusa_emulator.sh / docker-compose.moika.yml).

Зачем: собственный OPC UA-сервер ptusa (open62541, --opc r|rw) публикует только
<прибор>.state/.value — 398 из 1834 каналов станции, без режимов, уставок, техобъектов и
SYSTEM, а запись state на прибор не действует. Шлюз по требованию ходит к контроллеру только
по OPC UA, поэтому фасад берёт у прошивки ВСЁ её родным протоколом (driver-master) и отдаёт
по OPC UA:

  * адресное пространство — каналы из конфига станции (config/stations/*.yaml,
    tools/station_config.sh): узел ns=2;s=<прибор>.<поле> (LINE1V0.ST, OBJECT1.RT_PAR_F[12]),
    тип — dataType канала (INT32 → Int32, FLOAT → Float, STRING → String), запись — writable;
  * чтение — снимок GET_DEVICES_STATES раз в POLL_MS, исполняется настоящим Lua (lupa), значение
    t[прибор][поле] / t[прибор][массив][индекс]; SourceTimestamp — момент снимка;
  * качество — нет значения в снимке: BadNoData; нет связи с прошивкой: BadCommunicationError
    у всех узлов (сам OPC UA-сервер при этом жив — как у настоящего ПЛК с отвалившейся шиной);
  * запись клиента в узел — команда прошивке __<прибор>:set_cmd('<поле>', <индекс>, <значение>)
    ДО сохранения значения; клиент получает её исход: код 0 — Good, иначе BadInvalidState,
    нет связи — BadCommunicationError. Узел без writable — BadUserAccessDenied (asyncua).

Переменные окружения:
  PAC_HOST, PAC_PORT   — эмулятор ptusa (driver-master), по умолчанию ptusa:10000
  STATION_CONFIG       — конфиг станции, по умолчанию /config/station.yaml
  OPCUA_ENDPOINT       — адрес, который сервер АНОНСИРУЕТ клиентам; после discovery клиент идёт
                         именно по нему, поэтому он должен резолвиться у клиента: localhost — шлюз
                         на хосте (порт проброшен), имя сервиса — шлюз в сети compose. По умолчанию
                         opc.tcp://localhost:4840
  OPCUA_BIND           — на чём слушать, по умолчанию 0.0.0.0:4840 (не зависит от анонса)
  POLL_MS              — период снимка, по умолчанию 500
"""
import asyncio
import logging
import os
import re
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import lupa
import yaml
from asyncua import Server, ua

log = logging.getLogger("ptusa-opcua")

NAMESPACE_URI = "urn:savushkin:ptusa"

# Снимок присылает «прошивка» — то есть любой, кто ответит на порту PAC, поэтому Lua-стейт — песочница.
LUA_MEMORY_LIMIT = 64 * 1024 * 1024
LUA_TIME_BUDGET = 1.0          # секунд на разбор одного снимка (в норме — миллисекунды)
MAX_INFLATED_BYTES = 8 * 1024 * 1024   # потолок распакованного кадра (защита от zlib-бомбы)

_LUA_SANDBOX = """
local clock, sethook, error = os.clock, debug.sethook, error
local deadline = math.huge
sethook(function() if clock() > deadline then error('time limit exceeded') end end, '', 10000)
local function arm(seconds) deadline = clock() + seconds end
for _, name in ipairs{'python', 'os', 'io', 'debug', 'package', 'require', 'load', 'loadstring', 'loadfile',
                      'dofile', 'collectgarbage', 'coroutine', 'getfenv', 'setfenv', 'newproxy', 'module'} do
  _G[name] = nil
end
string.dump = nil
return arm
"""


def sandboxed_lua():
    """Lua-стейт для чужого скрипта: без доступа к Python (`python.eval`, `python.builtins` у lupa
    включены по умолчанию — это исполнение произвольного кода), без os/io/debug/load/байткода,
    с потолком памяти и времени. Возвращает (стейт, arm): `arm(секунды)` включает отсчёт времени
    перед исполнением скрипта; сам отсчёт скрипту недоступен."""
    lua = lupa.LuaRuntime(unpack_returned_tuples=True, register_eval=False, register_builtins=False,
                          max_memory=LUA_MEMORY_LIMIT)
    return lua, lua.execute(_LUA_SANDBOX)


def inflate(body: bytes, limit: int = MAX_INFLATED_BYTES) -> bytes:
    """zlib-распаковка с потолком размера: маленькое тело не должно раздуваться в гигабайты."""
    d = zlib.decompressobj()
    out = d.decompress(body, limit)
    if d.unconsumed_tail:
        raise ConnectionError(f"PAC: ответ распаковывается больше чем в {limit} байт")
    return out
BANNER = b"PAC accept"
STATUS_ERROR = 7
CMD_GET_INFO_ON_CONNECT = 10
CMD_GET_DEVICES_STATES = 101
CMD_EXEC_DEVICE_COMMAND = 102
# Поле: имя и необязательный индекс массива (в базе каналов бывает с пробелами: ST_CH[ 1 ]).
FIELD_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)(?:\[\s*(\d+)\s*\])?")

VARIANT_BY_TYPE = {"INT32": ua.VariantType.Int32, "FLOAT": ua.VariantType.Float,
                   "STRING": ua.VariantType.String, "BOOLEAN": ua.VariantType.Boolean}


@dataclass
class Channel:
    device: str
    field: str
    base: str
    index: Optional[int]
    data_type: str
    writable: bool
    node_name: str
    node: object = None
    variant: ua.VariantType = ua.VariantType.Float


def load_channels(path: str) -> list[Channel]:
    """Включённые каналы конфига станции с адресом в ПЛК (deviceName/fieldName)."""
    with open(path, encoding="utf-8") as f:
        root = yaml.safe_load(f)
    out, seen = [], set()
    for server in root["opcua"]["servers"]:
        for tag in server.get("tags", []):
            if not tag.get("enabled") or not tag.get("deviceName") or not tag.get("fieldName"):
                continue
            m = FIELD_RE.match(tag["fieldName"])
            if not m:
                log.warning("поле %s.%s не разобрано — пропуск", tag["deviceName"], tag["fieldName"])
                continue
            base, idx = m.group(1), int(m.group(2)) if m.group(2) else None
            node_name = tag["deviceName"] + "." + base + (f"[{idx}]" if idx is not None else "")
            if node_name in seen:
                continue
            seen.add(node_name)
            dt = str(tag.get("dataType", "FLOAT")).upper()
            out.append(Channel(tag["deviceName"], tag["fieldName"], base, idx, dt, bool(tag.get("writable")),
                               node_name, variant=VARIANT_BY_TYPE.get(dt, ua.VariantType.Float)))
    return out


def convert(value, variant: ua.VariantType):
    """Значение из Lua → значение узла его типа; None — не приводится."""
    if value is None:
        return None
    if variant == ua.VariantType.String:
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return value if isinstance(value, str) else str(value)
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return None
    if isinstance(value, bool):
        value = 1 if value else 0
    if not isinstance(value, (int, float)):
        return None
    if variant == ua.VariantType.Int32:
        return int(round(value))
    if variant == ua.VariantType.Boolean:
        return value != 0
    return float(value)


def scalar(value) -> str:
    """Значение для set_cmd: bool → 1/0, число — как есть."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return repr(value) if isinstance(value, float) else str(value)


def command_text(ch: Channel, value) -> str:
    """Команда прошивке. Индекс массива — отдельным аргументом: set_cmd('RT_PAR_F[12]', 1, v)
    ptusa принимает с кодом 0, но ничего не меняет (проверено на прошивке 2026.4.2.1)."""
    return f"__{ch.device}:set_cmd('{ch.base}', {ch.index if ch.index is not None else 1}, {scalar(value)})"


class PacClient:
    """driver-master: приветствие PAC accept, кадр 's',1,1,pidx,lenHi,lenLo + zlib(тело)."""

    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.host, self.port, self.timeout = host, port, timeout
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.pidx = 0
        self.lock = asyncio.Lock()

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), self.timeout)
        banner = await asyncio.wait_for(self.reader.readexactly(len(BANNER)), self.timeout)
        if banner != BANNER:
            raise ConnectionError(f"нет приветствия PAC accept: {banner!r}")
        # ptusa рвёт соединение, если клиент молчит > 300 мс после приветствия.
        await self._request(CMD_GET_INFO_ON_CONNECT)

    def close(self):
        if self.writer:
            self.writer.close()
        self.reader = self.writer = None

    @property
    def connected(self) -> bool:
        return self.writer is not None

    async def _request(self, cmd: int, extra: bytes = b"") -> bytes:
        self.pidx = (self.pidx + 1) % 256
        payload = bytes([cmd]) + extra
        self.writer.write(bytes([ord("s"), 1, 1, self.pidx, len(payload) >> 8, len(payload) & 0xFF]) + payload)
        await self.writer.drain()
        hdr = await asyncio.wait_for(self.reader.readexactly(5), self.timeout)
        body = await asyncio.wait_for(self.reader.readexactly((hdr[3] << 8) | hdr[4]), self.timeout)
        if hdr[0] != ord("s") or hdr[1] == STATUS_ERROR:
            raise ConnectionError(f"PAC: ответ {hdr!r} на команду {cmd}")
        return inflate(body) if body else b""

    async def snapshot(self) -> str:
        async with self.lock:
            body = await self._request(CMD_GET_DEVICES_STATES)
        # Первые 2 байта — devices_request_id; тело — C-строка с \0.
        return body[2:].split(b"\0", 1)[0].decode("utf-8", "replace")

    async def command(self, lua_text: str) -> int:
        async with self.lock:
            body = await self._request(CMD_EXEC_DEVICE_COMMAND, lua_text.encode("utf-8"))
        return int.from_bytes(body[:2], "little") if len(body) >= 2 else 0


class Bridge:
    def __init__(self, channels: list[Channel], pac: PacClient, poll_ms: int):
        self.channels = channels
        self.by_nodeid: dict[ua.NodeId, Channel] = {}
        self.pac = pac
        self.poll = poll_ms / 1000
        self.lua, self._arm = sandboxed_lua()
        self.server: Optional[Server] = None
        self.ns = 0
        self.stats = {"polls": 0, "commands": 0, "rejected": 0}

    async def build(self, endpoint: str, bind: Optional[tuple] = None):
        self.server = Server()
        await self.server.init()
        self.server.set_endpoint(endpoint)
        if bind is not None:
            # asyncua по умолчанию слушает на хосте из endpoint (имя сервиса → IP контейнера,
            # localhost → недоступен снаружи контейнера). Адрес привязки — отдельно от анонса.
            self.server.socket_address = bind
        self.server.set_server_name("ptusa (эмулятор мойки) — OPC UA-фасад")
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.ns = await self.server.register_namespace(NAMESPACE_URI)
        root = await self.server.nodes.objects.add_folder(ua.NodeId("PAC", self.ns), "PAC")
        folders = {}
        for ch in self.channels:
            folder = folders.get(ch.device)
            if folder is None:
                folder = await root.add_object(ua.NodeId(ch.device, self.ns), ch.device)
                folders[ch.device] = folder
            initial = "" if ch.variant == ua.VariantType.String else (False if ch.variant == ua.VariantType.Boolean else 0)
            node = await folder.add_variable(ua.NodeId(ch.node_name, self.ns), ch.node_name.split(".", 1)[1],
                                             ua.Variant(initial, ch.variant))
            if ch.writable:
                await node.set_writable()
            ch.node = node
            self.by_nodeid[node.nodeid] = ch
            await self._set(ch, None, ua.StatusCodes.BadWaitingForInitialData)
        self._intercept_writes()
        log.info("адресное пространство: %d каналов (%d на запись), %d объектов",
                 len(self.channels), sum(c.writable for c in self.channels), len(folders))

    async def _set(self, ch: Channel, value, status=ua.StatusCodes.Good, ts: Optional[datetime] = None):
        variant = ua.Variant(value, ch.variant) if value is not None else ua.Variant(None, ua.VariantType.Null)
        dv = ua.DataValue(variant, StatusCode_=ua.StatusCode(status),
                          SourceTimestamp=ts or datetime.now(timezone.utc),
                          ServerTimestamp=datetime.now(timezone.utc))
        await self.server.write_attribute_value(ch.node.nodeid, dv)

    def _intercept_writes(self):
        """Запись клиента → сначала команда прошивке; её исход — статус записи.

        asyncua 1.1.8 не даёт вернуть статус из value_setter, поэтому обёрнут сервис записи.
        Серверные обновления значений (write_attribute_value) идут мимо — в обход обёртки."""
        service = self.server.iserver.attribute_service
        original = service.write

        async def write(params, user=None):
            results = []
            for wv in params.NodesToWrite:
                ch = self.by_nodeid.get(wv.NodeId)
                if ch is not None and ch.writable and wv.AttributeId == ua.AttributeIds.Value:
                    status = await self.command(ch, wv.Value.Value.Value)
                    if not status.is_good():
                        results.append(status)
                        continue
                single = ua.WriteParameters()
                single.NodesToWrite = [wv]
                results.extend(await (original(single, user) if user is not None else original(single)))
            return results

        service.write = write

    async def command(self, ch: Channel, value) -> ua.StatusCode:
        self.stats["commands"] += 1
        if ch.variant == ua.VariantType.String or value is None or isinstance(value, str):
            self.stats["rejected"] += 1
            return ua.StatusCode(ua.StatusCodes.BadTypeMismatch)
        if not self.pac.connected:
            return ua.StatusCode(ua.StatusCodes.BadCommunicationError)
        text = command_text(ch, value)
        try:
            code = await self.pac.command(text)
        except Exception as e:
            log.warning("команда %s: нет связи с прошивкой: %s", text, e)
            self.pac.close()
            return ua.StatusCode(ua.StatusCodes.BadCommunicationError)
        if code != 0:
            self.stats["rejected"] += 1
            log.warning("команда %s: прошивка вернула код %d", text, code)
            return ua.StatusCode(ua.StatusCodes.BadInvalidState)
        log.info("✍ %s", text)
        return ua.StatusCode(ua.StatusCodes.Good)

    def _value(self, t, ch: Channel):
        dev = t[ch.device] if t is not None else None
        if dev is None:
            return None
        v = dev[ch.base]
        if ch.index is not None:
            v = v[ch.index] if lupa.lua_type(v) == "table" else None
        if lupa.lua_type(v) is not None:  # таблица/функция — не значение канала
            return None
        return v

    async def poll_once(self):
        text = await self.pac.snapshot()
        ts = datetime.now(timezone.utc)
        self._arm(LUA_TIME_BUDGET)
        self.lua.execute(text)
        t = self.lua.globals().t
        missing = 0
        for ch in self.channels:
            value = convert(self._value(t, ch), ch.variant)
            if value is None:
                missing += 1
                await self._set(ch, None, ua.StatusCodes.BadNoData, ts)
            else:
                await self._set(ch, value, ua.StatusCodes.Good, ts)
        self.stats["polls"] += 1
        return missing

    async def run(self):
        backoff, last_report = 1.0, 0.0
        async with self.server:
            log.info("OPC UA на %s, прошивка %s:%d, снимок раз в %d мс",
                     self.server.endpoint.geturl(), self.pac.host, self.pac.port, int(self.poll * 1000))
            while True:
                started = time.monotonic()
                try:
                    if not self.pac.connected:
                        await self.pac.connect()
                        log.info("🟢 связь с прошивкой %s:%d", self.pac.host, self.pac.port)
                        backoff = 1.0
                    missing = await self.poll_once()
                    if time.monotonic() - last_report > 60:
                        last_report = time.monotonic()
                        log.info("снимков %d, команд %d (отклонено %d), каналов без значения %d",
                                 self.stats["polls"], self.stats["commands"], self.stats["rejected"], missing)
                except Exception as e:
                    was = self.pac.connected
                    self.pac.close()
                    if was or backoff == 1.0:
                        log.warning("🔴 нет связи с прошивкой %s:%d: %s", self.pac.host, self.pac.port, e)
                    for ch in self.channels:
                        await self._set(ch, None, ua.StatusCodes.BadCommunicationError)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)
                    continue
                await asyncio.sleep(max(0.0, self.poll - (time.monotonic() - started)))


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for noisy in ("asyncua", "asyncua.server"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    channels = load_channels(os.environ.get("STATION_CONFIG", "/config/station.yaml"))
    pac = PacClient(os.environ.get("PAC_HOST", "ptusa"), int(os.environ.get("PAC_PORT", "10000")))
    bridge = Bridge(channels, pac, int(os.environ.get("POLL_MS", "500")))
    host, _, port = os.environ.get("OPCUA_BIND", "0.0.0.0:4840").rpartition(":")
    await bridge.build(os.environ.get("OPCUA_ENDPOINT", "opc.tcp://localhost:4840"), (host, int(port)))
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
