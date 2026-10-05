"""
Тесты OPC UA-фасада эмулятора ptusa (bridge.py) без настоящей прошивки: маленький фейковый
PAC-сервер (тот же проводной формат, что у ptusa: приветствие PAC accept, кадры 's',1,1,pidx,len,
zlib, тела — C-строки) + настоящий OPC UA-сервер фасада + настоящий OPC UA-клиент.

Запуск: cd ptusa-opcua && pip install -r requirements.txt pytest && python -m pytest -q tests
"""
import asyncio
import re
import socket
import sys
import zlib
from pathlib import Path

import pytest
import yaml
from asyncua import Client, ua

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bridge  # noqa: E402

NS = "urn:savushkin:ptusa"


# ------------------------------------------------------------------ чистые функции --

# ------------------------------------------------- песочница Lua: снимок присылает «прошивка» --

def test_lua_sandbox_has_no_python_or_host_access():
    lua, arm = bridge.sandboxed_lua()
    arm(1.0)
    for name in ("python", "os", "io", "debug", "package", "require", "load", "loadstring", "loadfile",
                 "dofile", "coroutine", "collectgarbage"):
        assert lua.eval(f"type({name})") == "nil", name
    assert lua.eval("type(string.dump)") == "nil"
    # Обычный разбор снимка работает: string/table/math на месте.
    lua.execute("t = {A = {V = math.max(1, 2), S = string.upper('ok')}}")
    assert lua.globals().t.A.V == 2


def test_lua_python_escape_attempts_fail():
    lua, arm = bridge.sandboxed_lua()
    for attack in ("python.eval('1+1')", "python.builtins.__import__('os')", "os.execute('true')",
                   "io.open('/etc/passwd')", "load('return 1')()", "debug.sethook()",
                   "require('os')", "string.dump(print)"):
        arm(1.0)
        with pytest.raises(Exception):
            lua.execute("x = " + attack)


def test_lua_endless_loop_is_stopped_by_time_budget():
    import time
    lua, arm = bridge.sandboxed_lua()
    arm(0.2)
    started = time.monotonic()
    with pytest.raises(Exception, match="time limit"):
        lua.execute("while true do end")
    assert time.monotonic() - started < 3
    # Лимит — на каждый скрипт: следующий снимок разбирается.
    arm(1.0)
    lua.execute("t = {X = 1}")
    assert lua.globals().t.X == 1


def test_lua_script_cannot_extend_its_own_time_budget():
    lua, arm = bridge.sandboxed_lua()
    arm(0.2)
    with pytest.raises(Exception):
        lua.execute("__arm = function() end; arm = function() end; while true do end")


def test_lua_memory_bomb_is_stopped():
    lua, arm = bridge.sandboxed_lua()
    arm(2.0)
    with pytest.raises(Exception):
        lua.execute("s = string.rep('x', 512 * 1024 * 1024)")


def test_zlib_bomb_is_rejected():
    bomb = zlib.compress(b"\0" * (64 * 1024 * 1024), 9)
    assert len(bomb) < 100 * 1024
    with pytest.raises(ConnectionError, match="распаковывается"):
        bridge.inflate(bomb)
    assert bridge.inflate(zlib.compress(b"t={}")) == b"t={}"


def test_field_and_node_names_normalize_indexes():
    m = bridge.FIELD_RE.match("ST_CH[ 3 ]")
    assert (m.group(1), int(m.group(2))) == ("ST_CH", 3)
    m = bridge.FIELD_RE.match("PAR_MAIN[2].P_CZAD_S")   # хвост после ] — подпись канала
    assert (m.group(1), int(m.group(2))) == ("PAR_MAIN", 2)
    assert bridge.FIELD_RE.match("ST").group(2) is None


def test_convert_by_variant_type():
    assert bridge.convert(1.0, ua.VariantType.Int32) == 1
    assert bridge.convert(2.6, ua.VariantType.Int32) == 3
    assert bridge.convert(3, ua.VariantType.Float) == 3.0
    assert bridge.convert("0 дн.", ua.VariantType.String) == "0 дн."
    assert bridge.convert(5.0, ua.VariantType.String) == "5"
    assert bridge.convert("abc", ua.VariantType.Float) is None
    assert bridge.convert(None, ua.VariantType.Int32) is None


def test_command_text_puts_array_index_in_its_own_argument():
    """set_cmd('RT_PAR_F[12]', 1, v) ptusa принимает с кодом 0, но ничего не меняет."""
    def ch(device, field, base, index):
        return bridge.Channel(device, field, base, index, "FLOAT", True, "n")
    assert bridge.command_text(ch("OBJECT1", "RT_PAR_F[12]", "RT_PAR_F", 12), 7.5) == \
        "__OBJECT1:set_cmd('RT_PAR_F', 12, 7.5)"
    assert bridge.command_text(ch("LINE1V0", "ST", "ST", None), 1) == "__LINE1V0:set_cmd('ST', 1, 1)"
    assert bridge.command_text(ch("LINE1V0", "M", "M", None), True) == "__LINE1V0:set_cmd('M', 1, 1)"
    assert bridge.command_text(ch("LINE1V0", "M", "M", None), 3.0) == "__LINE1V0:set_cmd('M', 1, 3)"


def test_load_channels_skips_disabled_and_dedups(tmp_path):
    cfg = tmp_path / "s.yaml"
    cfg.write_text(yaml.safe_dump({"opcua": {"servers": [{"tags": [
        {"deviceName": "LINE1G1", "fieldName": "ST_CH[ 1 ]", "dataType": "INT32", "enabled": True, "writable": False},
        {"deviceName": "LINE1G1", "fieldName": "ST_CH[1]", "dataType": "INT32", "enabled": True},        # дубль
        {"deviceName": "LINE1V0", "fieldName": "ST", "dataType": "INT32", "enabled": False},           # выключен
        {"deviceName": "LINE1V0", "fieldName": "M", "dataType": "INT32", "enabled": True, "writable": True},
    ]}]}}))
    chans = bridge.load_channels(str(cfg))
    assert [c.node_name for c in chans] == ["LINE1G1.ST_CH[1]", "LINE1V0.M"]
    assert [c.writable for c in chans] == [False, True]


# ---------------------------------------------------------------- фейковый PAC --

class FakePac:
    """Прошивка: приборы с полями, snapshot t=…, set_cmd меняет поле; код ≠ 0 для чужих приборов."""

    def __init__(self):
        self.devices = {"LINE1V0": {"M": 0, "ST": 0}, "LINE1TE1": {"V": 21.5, "P_CZ": 0.0}}
        self.rt_par_f = [0.0, 0.0, 0.0]
        self.commands: list[str] = []
        self.server = None
        self.writers: list[asyncio.StreamWriter] = []

    def snapshot(self) -> str:
        parts = ["t=\n{\n"]
        for name, fields in self.devices.items():
            parts.append(f"{name}={{" + ", ".join(f"{k}={v}" for k, v in fields.items()) + "},\n")
        parts.append("}\n")
        parts.append("t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n{\nRT_PAR_F=\n{\n"
                     + ", ".join(str(v) for v in self.rt_par_f) + ",\n},\n}\n")
        parts.append("t.SYSTEM =\n{\nUP_TIME=\"0 дн. 00:00:20\",\n}\n")
        return "".join(parts)

    def apply(self, cmd: str) -> int:
        self.commands.append(cmd)
        m = re.match(r"__(\w+):set_cmd\('(\w+)', (\d+), ([-\d.]+)\)", cmd)
        if not m:
            return 1
        dev, field, idx, value = m.group(1), m.group(2), int(m.group(3)), float(m.group(4))
        if dev == "OBJECT1" and field == "RT_PAR_F" and 1 <= idx <= len(self.rt_par_f):
            self.rt_par_f[idx - 1] = value
            return 0
        if dev in self.devices and field in self.devices[dev]:
            self.devices[dev][field] = int(value) if float(value).is_integer() and field in ("M", "ST") else value
            return 0
        return 1

    async def _client(self, reader, writer):
        self.writers.append(writer)
        writer.write(bridge.BANNER)
        try:
            while True:
                hdr = await reader.readexactly(6)
                payload = await reader.readexactly((hdr[4] << 8) | hdr[5])
                cmd, extra = payload[0], payload[1:]
                if cmd == 10:
                    body = b'protocol_version = 104; PAC_name = "FAKE";\n\0'
                elif cmd == 101:
                    body = b"\0\0" + self.snapshot().encode() + b"\0"
                else:
                    body = self.apply(extra.decode()).to_bytes(2, "little")
                packed = zlib.compress(body)
                writer.write(bytes([ord("s"), 12, hdr[3], len(packed) >> 8, len(packed) & 255]) + packed)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    async def start(self, port: int = 0) -> int:
        self.server = await asyncio.start_server(self._client, "127.0.0.1", port)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self):
        self.server.close()
        for w in self.writers:
            w.close()
        self.writers.clear()
        await self.server.wait_closed()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


CHANNELS = [
    ("LINE1V0", "ST", "INT32", True), ("LINE1V0", "M", "INT32", True), ("LINE1TE1", "V", "FLOAT", False),
    ("LINE1TE1", "P_CZ", "FLOAT", True), ("OBJECT1", "RT_PAR_F[2]", "FLOAT", True),
    ("SYSTEM", "UP_TIME", "STRING", False), ("LINE1V0", "NOPE", "INT32", False),   # поля нет в прошивке
    ("LINE9V9", "ST", "INT32", True),                                              # прибора нет в прошивке
]


def write_config(path: Path):
    path.write_text(yaml.safe_dump({"opcua": {"servers": [{"tags": [
        {"deviceName": d, "fieldName": f, "dataType": t, "enabled": True, "writable": w} for d, f, t, w in CHANNELS]}]}}))


async def stand(tmp_path, pac: FakePac):
    """Фасад на свободном порту против фейкового PAC; (bridge, bridge_task, pac_port, opc_port)."""
    cfg = tmp_path / "station.yaml"
    write_config(cfg)
    pac_port, opc_port = await pac.start(), free_port()
    br = bridge.Bridge(bridge.load_channels(str(cfg)), bridge.PacClient("127.0.0.1", pac_port, timeout=1.0), poll_ms=100)
    await br.build(f"opc.tcp://127.0.0.1:{opc_port}")
    task = asyncio.create_task(br.run())
    await asyncio.sleep(0.6)
    return br, task, pac_port, opc_port


async def node(client, name):
    return client.get_node(ua.NodeId(name, await client.get_namespace_index(NS)))


async def read(client, name):
    # Плохой статус узла — данные теста, а не исключение (по умолчанию asyncua его бросает).
    dv = await (await node(client, name)).read_data_value(raise_on_bad_status=False)
    return dv.Value.Value, dv.StatusCode.name


async def write(client, name, value, vtype):
    await (await node(client, name)).write_value(ua.DataValue(ua.Variant(value, vtype)))


# ---------------------------------------------------------------- фасад целиком --

def test_reads_writes_and_faults(tmp_path):
    async def scenario():
        pac = FakePac()
        br, task, pac_port, opc_port = await stand(tmp_path, pac)
        try:
            async with Client(f"opc.tcp://127.0.0.1:{opc_port}") as c:
                # --- чтение: типы, значения из снимка, массив по индексу, строка ---
                assert await read(c, "LINE1V0.ST") == (0, "Good")
                assert await read(c, "LINE1TE1.V") == (21.5, "Good")
                assert await read(c, "OBJECT1.RT_PAR_F[2]") == (0.0, "Good")
                assert await read(c, "SYSTEM.UP_TIME") == ("0 дн. 00:00:20", "Good")
                # нет поля / нет прибора в прошивке → BadNoData, а не выдуманное значение
                assert (await read(c, "LINE1V0.NOPE"))[1] == "BadNoData"
                assert (await read(c, "LINE9V9.ST"))[1] == "BadNoData"

                # --- запись = команда прошивке, значение приходит из следующего снимка ---
                await write(c, "LINE1V0.M", 1, ua.VariantType.Int32)
                await write(c, "LINE1V0.ST", 1, ua.VariantType.Int32)
                await write(c, "OBJECT1.RT_PAR_F[2]", 7.5, ua.VariantType.Float)
                await write(c, "LINE1TE1.P_CZ", 0.25, ua.VariantType.Float)
                await asyncio.sleep(0.4)
                assert pac.commands == ["__LINE1V0:set_cmd('M', 1, 1)", "__LINE1V0:set_cmd('ST', 1, 1)",
                                        "__OBJECT1:set_cmd('RT_PAR_F', 2, 7.5)", "__LINE1TE1:set_cmd('P_CZ', 1, 0.25)"]
                assert await read(c, "LINE1V0.ST") == (1, "Good")
                assert await read(c, "OBJECT1.RT_PAR_F[2]") == (7.5, "Good")
                assert await read(c, "LINE1TE1.P_CZ") == (0.25, "Good")

                # --- отказы: датчик (не writable), прибор, который прошивка не знает ---
                with pytest.raises(ua.UaStatusCodeError) as e:
                    await write(c, "LINE1TE1.V", 5.0, ua.VariantType.Float)
                assert e.value.code == ua.StatusCodes.BadUserAccessDenied
                before = len(pac.commands)
                with pytest.raises(ua.UaStatusCodeError) as e:
                    await write(c, "LINE9V9.ST", 1, ua.VariantType.Int32)
                assert e.value.code == ua.StatusCodes.BadInvalidState          # прошивка вернула код ≠ 0
                assert len(pac.commands) == before + 1
                assert await read(c, "LINE1V0.ST") == (1, "Good")              # значение не тронуто

                # --- обрыв связи с прошивкой: ВСЕ узлы BadCommunicationError, запись — тоже ---
                await pac.stop()
                await asyncio.sleep(1.5)
                assert (await read(c, "LINE1V0.ST"))[1] == "BadCommunicationError"
                assert (await read(c, "SYSTEM.UP_TIME"))[1] == "BadCommunicationError"
                with pytest.raises(ua.UaStatusCodeError) as e:
                    await write(c, "LINE1V0.M", 0, ua.VariantType.Int32)
                assert e.value.code == ua.StatusCodes.BadCommunicationError

                # --- восстановление: прошивка вернулась на прежнем порту — фасад переподключается ---
                await pac.start(pac_port)
                for _ in range(40):
                    await asyncio.sleep(0.25)
                    if (await read(c, "LINE1V0.ST"))[1] == "Good":
                        break
                assert await read(c, "LINE1V0.ST") == (1, "Good")
                await write(c, "LINE1V0.M", 0, ua.VariantType.Int32)
                await asyncio.sleep(0.4)
                assert await read(c, "LINE1V0.M") == (0, "Good")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await pac.stop()

    asyncio.run(asyncio.wait_for(scenario(), 60))


def test_string_and_wrong_type_writes_are_rejected(tmp_path):
    async def scenario():
        pac = FakePac()
        br, task, pac_port, opc_port = await stand(tmp_path, pac)
        try:
            async with Client(f"opc.tcp://127.0.0.1:{opc_port}") as c:
                # Строковый канал прошивке командой не пишется (set_cmd принимает только числа).
                ch = next(x for x in br.channels if x.node_name == "SYSTEM.UP_TIME")
                st = await br.command(ch, "x")
                assert st.value == ua.StatusCodes.BadTypeMismatch
                assert pac.commands == []
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await pac.stop()

    asyncio.run(asyncio.wait_for(scenario(), 30))
