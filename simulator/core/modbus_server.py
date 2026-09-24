"""
Modbus TCP-сервер симулятора (обёртка над pymodbus).

Держит один slave-контекст holding-регистров и крутит TCP-сервер в отдельном
daemon-потоке (StartTcpServer блокирующий). PLCSimulator на каждом цикле кладёт сюда
свежие значения через update_register (FLOAT занимает 2 регистра, little-endian по словам).
"""
import logging
import threading
# pymodbus 3.x: синхронный StartTcpServer переехал из pymodbus.server.sync в
# pymodbus.server (сам сервер по-прежнему блокирующий — крутим его в daemon-потоке).
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.device import ModbusDeviceIdentification

logger = logging.getLogger(__name__)


class ModbusServer:
    """Modbus TCP сервер для симуляции PLC"""

    def __init__(self, port: int = 5020):
        """Запоминает порт; контекст регистров и поток создаются позже в start()."""
        self.port = port
        self.context = None
        self.running = False
        self._server_thread = None

    def start(self):
        """Запуск Modbus TCP сервера в отдельном потоке"""
        logger.info(f"Starting Modbus TCP server on port {self.port}")

        # Создаём контекст с 10000 регистров
        slave_context = ModbusSlaveContext(zero_mode=True)
        self.context = ModbusServerContext(slaves=slave_context, single=True)

        # Информация об устройстве
        identity = ModbusDeviceIdentification()
        identity.VendorName = 'PLC Simulator'
        identity.ProductCode = 'PLC-EMU'
        identity.ProductName = 'Multi-Protocol PLC Simulator'
        identity.ModelName = 'PLC-EMU-1'
        identity.MajorMinorRevision = '1.0'

        self.running = True

        # Запускаем сервер в отдельном потоке. В pymodbus 3.x StartTcpServer
        # принимает только именованные аргументы (контекст — kwarg 'context',
        # не позиционно, как было в 2.x).
        self._server_thread = threading.Thread(
            target=StartTcpServer,
            kwargs={
                'context': self.context,
                'address': ("0.0.0.0", self.port),
                'identity': identity
            },
            daemon=True
        )
        self._server_thread.start()

        logger.info(f"Modbus TCP server started on port {self.port}")

    def update_register(self, address: int, value, data_type: str = "float32"):
        """Обновление значения в регистре"""
        if not self.context:
            return

        try:
            if data_type == "float32":
                # float32 занимает 2 регистра
                import struct
                packed = struct.pack('<f', float(value))
                reg1 = int.from_bytes(packed[0:2], 'little')
                reg2 = int.from_bytes(packed[2:4], 'little')
                # Записываем в holding registers (тип 3)
                self.context[0].setValues(3, address, [reg1, reg2])
            elif data_type == "int32":
                raw = int(value) & 0xFFFFFFFF
                self.context[0].setValues(3, address, [raw & 0xFFFF, (raw >> 16) & 0xFFFF])
            else:
                self.context[0].setValues(3, address, [int(value) & 0xFFFF])
        except Exception as e:
            logger.error(f"Failed to update Modbus register {address}: {e}")

    def read_registers(self, address: int, count: int = 1):
        """Прочитать holding-регистры из датастора.

        Нужно для обратного чтения: шлюз пишет команду прямо в датастор сервера,
        и без этого метода симулятор затёр бы её на следующем цикле опроса.
        """
        if not self.context:
            return None
        try:
            return self.context[0].getValues(3, address, count)
        except Exception as e:
            logger.error(f"Failed to read Modbus register {address}: {e}")
            return None

    def stop(self):
        """Остановка сервера"""
        self.running = False
        logger.info("Modbus TCP server stopped")