//! Клиенты протоколов против PLC-симулятора (`simulator/`): каждый тег
//! controllers.yaml читается своим протоколом и приходит с верным типом; запись команд
//! применяется и откатывается; запрещённое отклоняется.
//!
//!   cargo test --test simulator -- --ignored      (симулятор на SIM_HOST, см. tests/common)

mod common;

use std::time::Duration;

use common::{OP_TIMEOUT, controller, sim_host, toggled, type_matches};
use scada_gateway::modbus::{self, ModbusClient};
use scada_gateway::model::{ControllerKind, Protocol, Quality, TagValue};
use scada_gateway::opcua::{self, OpcConnection};
use scada_gateway::pac::{self, PacConnection};

const NEED_SIM: &str = "нужен симулятор: SIM_HOST=… cargo test -- --ignored";

// ---------------------------------------------------------------------------- OPC UA --

/// Клиент async-opcua берёт первый адрес из DNS: `localhost` → `::1`, а симулятор слушает только IPv4.
/// Шлюз сам выбирает доступный адрес (`opcua::reachable_url`), поэтому с `localhost` подключение есть.
#[tokio::test]
#[ignore = "нужен симулятор на этой машине: SIM_HOST=127.0.0.1 cargo test -- --ignored"]
async fn opcua_connects_through_localhost_name() {
    if !matches!(sim_host().as_str(), "127.0.0.1" | "localhost") {
        eprintln!("симулятор не на этой машине — проверка имени localhost пропущена");
        return;
    }
    let ctrl = controller(ControllerKind::OpcUa);
    let url = ctrl.endpoint.replace("127.0.0.1", "localhost");
    let tag = ctrl.tags.iter().find(|t| t.protocol == Protocol::OpcUa).expect("OPC UA-тег");
    let conn = OpcConnection::connect(&url, OP_TIMEOUT).await.unwrap_or_else(|e| panic!("{url}: {e:#}"));
    let values = conn.read(&[opcua::read_value_id(&tag.node_id).unwrap()]).await.unwrap();
    assert!(opcua::reading(&values[0]).1 == Quality::Good, "{url}: {:?}", values[0]);
    conn.close().await;
}

#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn opcua_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::OpcUa);
    let tags: Vec<_> = ctrl.tags.iter().filter(|t| t.protocol == Protocol::OpcUa).collect();
    let nodes: Vec<_> = tags.iter().map(|t| opcua::read_value_id(&t.node_id).unwrap()).collect();
    let conn = OpcConnection::connect(&ctrl.endpoint, OP_TIMEOUT).await.expect(NEED_SIM);

    let values = conn.read(&nodes).await.unwrap();
    assert_eq!(values.len(), tags.len());
    let problems: Vec<String> = tags
        .iter()
        .zip(&values)
        .filter_map(|(tag, dv)| {
            let (value, quality, _) = opcua::reading(dv);
            match value {
                Some(v) if quality == Quality::Good && type_matches(&tag.data_type, &v) => None,
                other => Some(format!("{} ({}): {other:?} {quality:?}", tag.name, tag.data_type)),
            }
        })
        .collect();
    conn.close().await;
    assert!(
        problems.is_empty(),
        "{} из {} тегов не прочитаны как надо: {:?}",
        problems.len(),
        tags.len(),
        &problems[..problems.len().min(10)]
    );
}

#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn opcua_write_applies_and_readonly_is_rejected() {
    let ctrl = controller(ControllerKind::OpcUa);
    let conn = OpcConnection::connect(&ctrl.endpoint, OP_TIMEOUT).await.expect(NEED_SIM);

    // M3.M — чистый актуатор (RW, без источника данных): симулятор его не перетирает.
    let rw = ctrl.tags.iter().find(|t| t.name.ends_with(".M_M.M3.M")).expect("тег M3.M");
    assert!(rw.writable);
    let node = opcua::parse_node_id(&rw.node_id).unwrap();
    let read = || async {
        opcua::reading(&conn.read(&[opcua::read_value_id(&rw.node_id).unwrap()]).await.unwrap()[0]).0.unwrap()
    };
    let before = read().await;
    let target = toggled(&before);
    let status = conn.write(node.clone(), opcua::to_variant(&rw.data_type, &target).unwrap()).await.unwrap();
    assert!(status.is_good(), "запись в RW-узел: {status}");
    assert_eq!(read().await, target, "узел должен вернуть записанное значение");
    let status = conn.write(node, opcua::to_variant(&rw.data_type, &before).unwrap()).await.unwrap();
    assert!(status.is_good());
    assert_eq!(read().await, before);

    // Показание датчика: узел только на чтение — сервер отклоняет запись.
    let ro = ctrl
        .tags
        .iter()
        .find(|t| !t.writable && t.protocol == Protocol::OpcUa && t.data_type == "FLOAT")
        .expect("RO-тег");
    let status = conn
        .write(
            opcua::parse_node_id(&ro.node_id).unwrap(),
            opcua::to_variant(&ro.data_type, &TagValue::F64(1.0)).unwrap(),
        )
        .await
        .unwrap();
    assert!(!status.is_good(), "запись в RO-узел {} должна быть отклонена", ro.name);
    assert_eq!(opcua::classify_write_status(status), "REJECTED_NOT_WRITABLE", "статус {status}");
    conn.close().await;
}

// ---------------------------------------------------------------------------- Modbus --

#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn modbus_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::Modbus);
    let tags: Vec<_> = ctrl.tags.iter().filter(|t| t.protocol == Protocol::Modbus).cloned().collect();
    let mut blocks = modbus::plan_blocks(&tags);
    let (host, port) = modbus::endpoint(&ctrl.endpoint);
    let mut client = ModbusClient::new(host, port, tags[0].modbus_unit_id, OP_TIMEOUT);

    let values = client.read(&mut blocks).await.expect(NEED_SIM);
    assert_eq!(values.len(), tags.len(), "каждый тег попал в план чтения");
    let problems: Vec<String> = values
        .iter()
        .filter(|(tag, v)| !v.as_ref().is_some_and(|v| type_matches(&tag.data_type, v)))
        .map(|(tag, v)| format!("{} ({}): {v:?}", tag.name, tag.data_type))
        .collect();
    assert!(problems.is_empty(), "не прочитаны: {problems:?}");
    assert!(blocks.len() < tags.len() / 10, "чтение блоками: {} запросов на {} тегов", blocks.len(), tags.len());
}

// ------------------------------------------------------------------------------- PAC --

#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn pac_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect(NEED_SIM);
    conn.poll_states().await.unwrap();

    let problems: Vec<String> = ctrl
        .tags
        .iter()
        .filter(|t| t.protocol == Protocol::Pac)
        .filter_map(|t| {
            let v = conn.read_value(t.device_name.as_deref()?, t.field_name.as_deref()?, &t.data_type);
            match v {
                Some(v) if type_matches(&t.data_type, &v) => None,
                other => Some(format!("{} ({}): {other:?}", t.name, t.data_type)),
            }
        })
        .collect();
    assert!(problems.is_empty(), "нет в снимке t[прибор][поле]: {problems:?}");
}

#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn pac_write_applies_and_unknown_device_is_rejected() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect(NEED_SIM);

    // LINE1V0.M — чистый актуатор: пишем противоположное и возвращаем как было.
    conn.poll_states().await.unwrap();
    let before = conn.read_value("LINE1V0", "M", "INT32").expect("LINE1V0.M в снимке");
    let target = toggled(&before);
    assert_eq!(conn.exec_command("LINE1V0", "M", &target).await.unwrap(), 0, "PAC применяет set_cmd");
    // Снимок симулятор обновляет раз в update_rate (~0,5 с).
    let mut seen = None;
    for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        conn.poll_states().await.unwrap();
        seen = conn.read_value("LINE1V0", "M", "INT32");
        if seen.as_ref() == Some(&target) {
            break;
        }
    }
    assert_eq!(seen, Some(target), "после записи M должен смениться");
    assert_eq!(conn.exec_command("LINE1V0", "M", &before).await.unwrap(), 0);

    assert_ne!(
        conn.exec_command("NO_SUCH_DEVICE", "M", &TagValue::Int(1)).await.unwrap(),
        0,
        "несуществующий прибор — код ошибки"
    );
}
