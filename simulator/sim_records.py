#!/usr/bin/env python3
"""
Симулятор-КОНТРОЛЛЕР станции BN1_MCA1 (архитектура «сырые записи приборов»).

Отдаёт данные так, как настоящий ПЛК ptusa_main: на каждый прибор — ОДНА строка
device::save_device вида

    ИМЯ={ПОЛЕ=знач, ПОЛЕ=знач, ...}

Строка публикуется как значение одного OPC UA string-узла (NodeId ns=2;s=REC:<key>).
Значения полей берутся из 5-суточного архива BN1_MCA1 (replay в реальном времени, loop).

Расщепление записи на каналы, привязку к базе каналов и отправку в Kafka делает
ШЛЮЗ (Spring Boot) — как реальный драйвер PAC_easy_drv_LZ. Здесь только «контроллер».

Запуск:  python sim_records.py config/records_config.yaml
Env:     OPCUA_ENDPOINT (напр. opc.tcp://simulator:4840), иначе из конфига.
"""
import asyncio
import logging
import math
import os
import sys
from pathlib import Path

import yaml
from asyncua import Server, ua

from core.archive_replay import ArchiveReplay

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("sim_records")


def fmt_value(raw, dtype):
    """Формат значения поля как в ptusa: bool→0/1, float→целое или 2 знака."""
    if raw is None:
        return "0"
    if dtype == "bool":
        return "1" if float(raw) != 0.0 else "0"
    val = float(raw)
    frac, _ = math.modf(val)
    return f"{val:.0f}" if frac == 0.0 else f"{val:.2f}"


class RecordController:
    """OPC UA-контроллер «сырых записей приборов».

    На каждый прибор поднимает один string-узел (NodeId ns=2;s=REC:<key>) и на каждом
    цикле пишет туда строку ИМЯ={поле=знач,...}, собранную из архивных значений полей.
    Расщепление строки на каналы делает шлюз — здесь только имитация контроллера."""

    def __init__(self, config: dict):
        """Разбирает конфиг (plc + devices + replay) и готовит OPC UA-сервер и проигрыватель архива."""
        plc = config['plc']
        self.plc_id = plc['id']
        self.name = plc['name']
        self.endpoint = os.environ.get('OPCUA_ENDPOINT', plc['endpoint'])
        self.update_rate = plc.get('update_rate', 0.5)
        self.devices = config['devices']

        rc = config['replay']
        self.replay = ArchiveReplay(
            data_path=rc['data_path'], speed=rc.get('speed', 1.0),
            loop=rc.get('loop', True), base_dir=Path(__file__).resolve().parent)

        self.server = Server()
        self.ns = None
        self.dev_nodes = {}   # key -> opcua variable node

    def build_record(self, device, offset) -> str:
        """Собрать строку ИМЯ={поле=знач, ...} из архивных значений на момент offset."""
        parts = []
        for f in device['fields']:
            cid = f['cid']
            raw = self.replay.value_at(cid, offset) if self.replay.has(cid) else None
            parts.append(f"{f['field']}={fmt_value(raw, f['dtype'])}")
        return f"{device['name']}={{{', '.join(parts)}}}"

    async def init_server(self):
        """Поднимает OPC UA-сервер и заводит по одному string-узлу REC:<key> на прибор."""
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.name)
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.ns = await self.server.register_namespace(f"http://{self.plc_id}")

        objects = self.server.get_objects_node()
        plc_node = await objects.add_object(self.ns, self.plc_id)

        offset = 0.0
        for dev in self.devices:
            key = dev['key']
            node_id = ua.NodeId(f"REC:{key}", self.ns)   # ns=2;s=REC:<key>
            init = self.build_record(dev, offset)
            var = await plc_node.add_variable(node_id, dev['name'], init)
            await var.set_writable(False)
            self.dev_nodes[key] = var
        log.info("OPC UA: %d приборов-записей на %s", len(self.dev_nodes), self.endpoint)

    async def update_loop(self):
        """Бесконечный цикл: раз в update_rate пишет свежую запись каждого прибора в его узел."""
        while True:
            offset = self.replay.current_offset()
            for dev in self.devices:
                node = self.dev_nodes.get(dev['key'])
                if node is None:
                    continue
                try:
                    await node.write_value(self.build_record(dev, offset))
                except Exception as e:
                    log.debug("write %s: %s", dev['key'], e)
            await asyncio.sleep(self.update_rate)

    async def start(self):
        """Грузит архив, поднимает сервер и запускает цикл записи (блокирует до отмены)."""
        self.replay.load()
        await self.init_server()
        async with self.server:
            self.replay.start()
            log.info("Контроллер запущен: %s (приборов %d, архив %.1f сут, speed=%.1f)",
                     self.endpoint, len(self.devices),
                     self.replay.duration / 86400, self.replay.speed)
            # пример одной записи в лог
            if self.devices:
                log.info("пример записи: %s", self.build_record(self.devices[0],
                                                                self.replay.current_offset()))
            await self.update_loop()


async def main():
    """Точка входа: путь к конфигу из argv (по умолчанию config/records_config.yaml)."""
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config/records_config.yaml"
    config = yaml.safe_load(open(Path(__file__).parent / cfg_path, encoding='utf-8'))
    ctrl = RecordController(config)
    try:
        await ctrl.start()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("stop")
