"""
Разовый скрипт-обходчик OPC UA (ручная отладка): подключается к симулятору на
localhost:4840, находит узел ПЛК и печатает дерево DataBlock → теги с их nodeId и
значениями. Не часть рантайма — удобный чек «сервер жив и отдаёт данные».
Запуск: python tools/opcua_tree.py (нужен синхронный клиент: pip install opcua).
"""
from opcua import Client
client = Client("opc.tcp://localhost:4840")
client.connect()
objects = client.get_objects_node()
plc = objects.get_child(["2:S7-1200-SIM-001"])
print("PLC Node:", plc.nodeid)

for db in plc.get_children():
    print(f"\n{db.get_browse_name()}: {db.nodeid}")
    for tag in db.get_children():
        print(f"  {tag.get_browse_name()}: {tag.nodeid}")
        print(f"    Value: {tag.get_value()}")
client.disconnect()