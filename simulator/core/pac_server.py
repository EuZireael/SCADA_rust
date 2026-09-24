"""
PAC-контроллер симулятора — сервер протокола driver-master (Savushkin/ptusa).

Третий тип контроллера рядом с OPC UA и Modbus. Говорит на «родном» протоколе PAC так,
как его видно у эмулятора ptusa 2026.4.2.1 (сборка ptusa_main под ПК, проект BN1-МСА1):
после accept — приветствие `PAC accept`, дальше кадры `'s' + ServiceID + FrameType +
pidx + BE16-длина + payload`, тело ОТВЕТА = zlib(Lua), статус успеха 12.

Поддерживаем версию протокола 104 (zlib + UTF-8) — текущую у реальных PAC; QuickLZ
(легаси v102) намеренно не тащим. Сервер держит СНИМОК значений тегов (его обновляет
PLCSimulator каждый цикл) и на запрос генерит из него Lua. Крутится в отдельном
daemon-потоке (ThreadingTCPServer), как Modbus-сервер.

Реализованные команды (device_communicator::CMD):
  GET_INFO_ON_CONNECT(10)  — версия протокола, имя PAC, CRC параметров (handshake);
  GET_DEVICES(100)         — объектная модель (устройства→поля);
  GET_DEVICES_STATES(101)  — снимок по приборам: t={LINE1V0={M=0, ST=1}, ...} (основной опрос);
  EXEC_DEVICE_COMMAND(102) — запись: Lua-команда set_cmd применяется к RW-тегу, в ответ
                             код результата LE16 (0 — применено, 1 — нет такого RW-поля);
  GET_PAC_ERRORS(103)      — заглушка «нет ошибок».
Остальное — пустой ответ со статусом успеха, как у ptusa.
"""
import logging
import re
import socketserver
import struct
import threading
import zlib

logger = logging.getLogger(__name__)

# Версия протокола: 104 = zlib + UTF-8 (текущая у реальных PAC). См. memory pac-driver-protocol.
PROTOCOL_VERSION = 104

# Коды команд (device_communicator::CMD из driver-master/common/PAC-driver/g_device.h).
CMD_GET_INFO_ON_CONNECT = 10
CMD_GET_DEVICES = 100
CMD_GET_DEVICES_STATES = 101
CMD_EXEC_DEVICE_COMMAND = 102
CMD_GET_PAC_ERRORS = 103

BANNER = b"PAC accept"     # приветствие ptusa сразу после accept, до первого кадра.
NET_ID = ord('s')          # магический байт кадра (заголовок[0]).
STATUS_OK = 12             # так помечает успешный ответ ptusa.
STATUS_ERROR = 7           # драйвер трактует ответ[1] == 7 как ошибку.
REQUEST_HEADER_LEN = 6     # 's', ServiceID, FrameType, pidx, lenHi, lenLo.
MAX_BODY = 0xFFFF          # длина ответа — 2 байта → сжатое тело ≤ 65535 байт.

# Код результата EXEC_DEVICE_COMMAND (LE16), как у ptusa: 0 — применено, 1 — нет.
EXEC_APPLIED = struct.pack("<H", 0)
EXEC_FAILED = struct.pack("<H", 1)

# Поле-массив канала: RT_PAR_F[12] или PAR_MAIN[1].P_CZAD_S (хвост — подпись канала).
_ARRAY_FIELD_RE = re.compile(r"^(?P<name>[^\[\]]+)\[(?P<idx>\d+)\]")
_LUA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Разбор Lua-команды записи: `__1V1:set_cmd('ST', 1, 1)` или `..., 'value')`.
# Ведущие подчёркивания (у имён с цифры) съедаются, dev='1V1', field='ST', val='1'.
_SET_CMD_RE = re.compile(
    r"_*(?P<dev>\w+):set_cmd\(\s*'(?P<field>\w+)'\s*,\s*\d+\s*,\s*'?(?P<val>[^')]+)'?\s*\)")


def _recvall(sock, n):
    """Прочитать РОВНО n байт (TCP может отдавать частями). None при закрытии соединения."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def _lua_value(value):
    """Значение тега → Lua-литерал. bool→1/0 (в протоколе это T_NUMBER), float→компактно,
    строка (рецепт, список программ) → в кавычках, как CUR_REC='…' у ptusa."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
        return f"'{escaped}'"
    return _lua_value(float(value))


def _lua_key(name):
    """Ключ Lua-таблицы: идентификатор как есть (LINE1V0), иначе ["1V1"]."""
    if _LUA_NAME_RE.match(name):
        return name
    return '["' + name.replace("\\", "\\\\").replace('"', '\\"') + '"]'


def _parse_scalar(text):
    """Строку значения из set_cmd → число, если можно (иначе оставить строкой)."""
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


class _Handler(socketserver.BaseRequestHandler):
    """Обработчик одного TCP-соединения: цикл «прочитать кадр → ответить», пока не закроют."""

    def handle(self):
        sock = self.request
        pac = self.server.pac          # ссылка на PACServer (снимок/имя/устройства)
        peer = self.client_address
        logger.info(f"PAC: драйвер подключился {peer}")
        try:
            sock.sendall(BANNER)
            while True:
                header = _recvall(sock, REQUEST_HEADER_LEN)
                if header is None:
                    break
                if header[0] != NET_ID:
                    logger.warning(f"PAC: неверный заголовок кадра от {peer} — рву связь")
                    break
                pidx = header[3]
                length = (header[4] << 8) | header[5]
                payload = _recvall(sock, length) if length else b""
                if payload is None:
                    break
                cmd = payload[0] if payload else 0
                status, body = pac.handle_command(cmd, payload)
                self._send_response(sock, pidx, status, body)
        except (ConnectionError, OSError) as e:
            logger.debug(f"PAC: соединение {peer} закрыто: {e}")
        finally:
            logger.info(f"PAC: драйвер отключился {peer}")

    @staticmethod
    def _send_response(sock, pidx, status, body):
        """Кадр ответа: 's', статус, pidx(эхо), BE16-длина сжатого тела, zlib(body)."""
        packed = zlib.compress(body) if status == STATUS_OK and body else b""
        if len(packed) > MAX_BODY:
            logger.error(f"PAC: сжатый ответ {len(packed)}B > {MAX_BODY} — не влезает в длину")
            sock.sendall(bytes([NET_ID, STATUS_ERROR, pidx, 0, 0]))
            return
        length = len(packed)
        header = bytes([NET_ID, status, pidx, (length >> 8) & 0xFF, length & 0xFF])
        sock.sendall(header + packed)


class _TCPServer(socketserver.ThreadingTCPServer):
    daemon_threads = True          # потоки-соединения не держат процесс при выходе
    allow_reuse_address = True     # быстрый рестарт без TIME_WAIT


class PACServer:
    """Сервер протокола driver-master: держит снимок значений тегов и отвечает Lua."""

    def __init__(self, port=10000, pac_name="PAC", params_crc=0):
        """Запоминает порт/имя PAC; снимок, устройства и поток создаются в start()."""
        self.port = port
        self.pac_name = pac_name
        self.params_crc = params_crc
        self.running = False
        self._server = None
        self._thread = None
        self._lock = threading.Lock()
        self._values = {}          # {node_id(str): value} — снимок, пишет PLCSimulator
        self._devices = []         # [{device, dev_type, fields:[{field, node_id}]}]
        self._request_id = 1       # devices_request_id: конфиг сима не меняется → константа
        self.on_write = None       # callback(device, field, value) на EXEC_DEVICE_COMMAND

    # ------------------------------------------------------- обновление данных --
    def set_devices(self, devices):
        """Задать объектную модель (устройства→поля) для CMD_GET_DEVICES."""
        with self._lock:
            self._devices = list(devices)

    def update_snapshot(self, values):
        """Обновить снимок значений (node_id→value). PLCSimulator зовёт каждый цикл."""
        with self._lock:
            self._values = dict(values)

    # ------------------------------------------------------------ жизненный цикл --
    def start(self):
        """Поднять TCP-сервер в daemon-потоке (блокирующий serve_forever)."""
        self._server = _TCPServer(("0.0.0.0", self.port), _Handler)
        self._server.pac = self
        self.running = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(f"PAC server (driver-master v{PROTOCOL_VERSION}) started on port {self.port}")

    def stop(self):
        """Остановить сервер и закрыть слушающий сокет."""
        self.running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        logger.info("PAC server stopped")

    # --------------------------------------------------------- обработка команд --
    def handle_command(self, cmd, payload):
        """Диспетчер команды драйвера. Возвращает (status, body_bytes) ДО сжатия."""
        if cmd == CMD_GET_INFO_ON_CONNECT:
            return STATUS_OK, self._build_info()
        if cmd == CMD_GET_DEVICES:
            return STATUS_OK, self._build_devices()
        if cmd == CMD_GET_DEVICES_STATES:
            return STATUS_OK, self._build_states()
        if cmd == CMD_EXEC_DEVICE_COMMAND:
            return STATUS_OK, EXEC_APPLIED if self._apply_command(payload[1:]) else EXEC_FAILED
        if cmd == CMD_GET_PAC_ERRORS:
            return STATUS_OK, b"errors={}\n"  # заглушка: ошибок нет
        # ptusa на незнакомую команду отвечает пустым телом со статусом успеха.
        logger.warning(f"PAC: неизвестная команда {cmd}")
        return STATUS_OK, b""

    def _build_info(self):
        """CMD_GET_INFO_ON_CONNECT: версия протокола, имя PAC, CRC (без префикса request_id)."""
        name = self.pac_name.replace("\\", "\\\\").replace('"', '\\"')
        lua = (f'protocol_version = {PROTOCOL_VERSION}; PAC_name = "{name}"; '
               f"is_reset_params = 0;params_CRC={self.params_crc};\n")
        return lua.encode("utf-8")

    def _with_request_id(self, lua_bytes):
        """Формат ответов devices/states: 2 байта devices_request_id (LE) + Lua-текст."""
        with self._lock:
            rid = self._request_id
        return struct.pack("<H", rid) + lua_bytes

    def _build_devices(self):
        """CMD_GET_DEVICES: объектная модель — devices['1V1']={type='V',fields={'ST','M'}}."""
        with self._lock:
            devices = list(self._devices)
        parts = ["devices={}\n"]
        for d in devices:
            fields = ",".join(f"'{f['field']}'" for f in d["fields"])
            parts.append(f"devices['{d['device']}']="
                         f"{{type='{d['dev_type']}',fields={{{fields}}}}}\n")
        return self._with_request_id("".join(parts).encode("utf-8"))

    def _build_states(self):
        """CMD_GET_DEVICES_STATES: снимок по приборам, как device_manager::save_device у ptusa:
        t={ LINE1V0={M=0, ST=1}, OBJECT1={CMD=0, RT_PAR_F={[12]=0.5}}, ... }. Канал читают
        как t[прибор][поле]; поле-массив RT_PAR_F[12] уходит элементом таблицы RT_PAR_F."""
        with self._lock:
            values = dict(self._values)
            devices = list(self._devices)
        parts = ["t=\n\t{\n"]
        for d in devices:
            fields, arrays = [], {}
            for f in d["fields"]:
                if f["node_id"] not in values:
                    continue
                value = values[f["node_id"]]
                m = _ARRAY_FIELD_RE.match(f["field"])
                if m:
                    arrays.setdefault(m.group("name"), {})[int(m.group("idx"))] = value
                else:
                    fields.append(f"{_lua_key(f['field'])}={_lua_value(value)}")
            for name, items in arrays.items():
                inner = ", ".join(f"[{i}]={_lua_value(items[i])}" for i in sorted(items))
                fields.append(f"{_lua_key(name)}={{{inner}}}")
            if fields:
                parts.append(f"\t{_lua_key(d['device'])}={{{', '.join(fields)}}},\n")
        parts.append("\t}\n")
        return self._with_request_id("".join(parts).encode("utf-8"))

    def _apply_command(self, raw):
        """CMD_EXEC_DEVICE_COMMAND: разобрать set_cmd и применить к RW-тегу.
        True — применено; False — команда не распознана или такого RW-поля нет."""
        text = raw.decode("utf-8", errors="replace")
        m = _SET_CMD_RE.search(text)
        if not m:
            logger.info(f"PAC: команда записи не распознана: {text!r}")
            return False
        dev, field, val = m.group("dev"), m.group("field"), _parse_scalar(m.group("val"))
        logger.info(f"PAC: запись {dev}.{field} = {val}")
        return bool(self.on_write and self.on_write(dev, field, val))
