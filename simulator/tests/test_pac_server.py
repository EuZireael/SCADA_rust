"""
Тесты PAC-сервера симулятора (core/pac_server.py) на соответствие ptusa: формат снимка
t[прибор][поле], приветствие, статус успеха и код результата записи. Эталон — ответы
эмулятора ptusa 2026.4.2.1 с проектом станции BN1-МСА1.
"""
import socket
import struct
import zlib

import pytest

from core.pac_server import (BANNER, CMD_EXEC_DEVICE_COMMAND, CMD_GET_DEVICES_STATES,
                             CMD_GET_INFO_ON_CONNECT, EXEC_APPLIED, EXEC_FAILED, STATUS_OK,
                             PACServer)

DEVICES = [
    {"device": "LINE1V0", "dev_type": "V",
     "fields": [{"field": "M", "node_id": "460"}, {"field": "ST", "node_id": "385"}]},
    {"device": "OBJECT1", "dev_type": "Параметры_линии",
     "fields": [{"field": "RT_PAR_F[12]", "node_id": "96"},
                {"field": "RT_PAR_F[7]", "node_id": "91"},
                {"field": "PAR_MAIN[1].P_CZAD_S", "node_id": "6"},
                {"field": "CUR_REC", "node_id": "78"}]},
    {"device": "1V1", "dev_type": "V", "fields": [{"field": "ST", "node_id": "9001"}]},
]
VALUES = {"460": 0, "385": 1, "96": 0.5, "91": 1.25, "6": 1.0, "78": "Танк №1 'сырой'", "9001": True}


@pytest.fixture
def pac():
    server = PACServer(port=0, pac_name="BN1-МСА1")
    server.set_devices(DEVICES)
    server.update_snapshot(VALUES)
    return server


def states_lua(pac):
    status, body = pac.handle_command(CMD_GET_DEVICES_STATES, bytes([CMD_GET_DEVICES_STATES]))
    assert status == STATUS_OK
    assert body[:2] == struct.pack("<H", 1), "первые 2 байта — devices_request_id"
    return body[2:].decode("utf-8")


def test_states_are_keyed_by_device_like_ptusa(pac):
    lua = states_lua(pac)
    assert lua.startswith("t=\n\t{\n")
    assert "\tLINE1V0={M=0, ST=1},\n" in lua
    assert "tags[" not in lua, "адресация по channelId осталась в прошлом"


def test_array_fields_become_lua_tables(pac):
    lua = states_lua(pac)
    # PAR_MAIN[1].P_CZAD_S — хвост после ] подпись канала, в ПЛК это PAR_MAIN[1].
    assert "PAR_MAIN={[1]=1}" in lua
    assert "RT_PAR_F={[7]=1.25, [12]=0.5}" in lua


def test_strings_are_quoted_and_names_escaped(pac):
    lua = states_lua(pac)
    assert "CUR_REC='Танк №1 \\'сырой\\''" in lua
    assert '["1V1"]={ST=1}' in lua, "имя с цифры — не Lua-идентификатор"


def test_exec_command_reports_result_code(pac):
    applied = []
    pac.on_write = lambda dev, field, val: applied.append((dev, field, val)) or dev == "LINE1V0"

    status, body = pac.handle_command(CMD_EXEC_DEVICE_COMMAND,
                                      bytes([CMD_EXEC_DEVICE_COMMAND]) + b"__LINE1V0:set_cmd('M', 1, 1)")
    assert (status, body) == (STATUS_OK, EXEC_APPLIED)
    assert applied == [("LINE1V0", "M", 1)]

    _, body = pac.handle_command(CMD_EXEC_DEVICE_COMMAND,
                                 bytes([CMD_EXEC_DEVICE_COMMAND]) + b"__NO_SUCH:set_cmd('M', 1, 1)")
    assert body == EXEC_FAILED

    _, body = pac.handle_command(CMD_EXEC_DEVICE_COMMAND, bytes([CMD_EXEC_DEVICE_COMMAND]) + b"garbage")
    assert body == EXEC_FAILED


def test_unknown_command_is_empty_success(pac):
    assert pac.handle_command(55, bytes([55])) == (STATUS_OK, b"")


def test_socket_exchange_starts_with_banner(pac):
    pac.start()
    try:
        port = pac._server.server_address[1]
        with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
            assert recvn(s, len(BANNER)) == BANNER

            s.sendall(bytes([ord("s"), 1, 1, 7, 0, 1, CMD_GET_INFO_ON_CONNECT]))
            hdr = recvn(s, 5)
            assert hdr[:3] == bytes([ord("s"), STATUS_OK, 7]), "эхо pidx и статус успеха ptusa"
            info = zlib.decompress(recvn(s, (hdr[3] << 8) | hdr[4])).decode("utf-8")
            assert info.startswith("protocol_version = 104;")
            assert 'PAC_name = "BN1-МСА1"' in info
    finally:
        pac.stop()


def recvn(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        assert chunk, "соединение закрыто раньше времени"
        buf += chunk
    return buf
