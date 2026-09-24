//! Конфигурация шлюза (`config/controllers.yaml`) и симулятора
//! (`simulator/config/replay_config.yaml`) описывают одни и те же каналы. Расхождение
//! (канал есть только с одной стороны, другой тип, адрес или право записи) иначе проявится
//! только BAD-качеством или отказом команды на стенде. Внешние системы не нужны.

use std::collections::HashMap;

use scada_gateway::config;
use serde::Deserialize;

#[derive(Deserialize)]
struct SimFile {
    plc: SimPlc,
}

#[derive(Deserialize)]
struct SimPlc {
    data_blocks: Vec<SimBlock>,
}

#[derive(Deserialize)]
struct SimBlock {
    tags: Vec<SimTag>,
}

#[derive(Deserialize)]
struct SimTag {
    address: String,
    #[serde(rename = "type")]
    kind: String,
    protocol: String,
    access: String,
    device: Option<String>,
    field: Option<String>,
    modbus_address: Option<i32>,
    modbus_type: Option<String>,
}

fn path(rel: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel)
}

#[test]
fn gateway_and_simulator_describe_the_same_channels() {
    let gateway =
        config::parse_controllers(&std::fs::read_to_string(path("config/controllers.yaml")).unwrap()).unwrap();
    let sim: SimFile =
        serde_yaml_ng::from_str(&std::fs::read_to_string(path("simulator/config/replay_config.yaml")).unwrap())
            .unwrap();
    let sim_tags: HashMap<String, SimTag> =
        sim.plc.data_blocks.into_iter().flat_map(|b| b.tags).map(|t| (t.address.clone(), t)).collect();

    let mut problems = Vec::new();
    let mut seen = 0;
    for tag in gateway.iter().flat_map(|s| &s.tags) {
        seen += 1;
        let id = tag.channel_id.expect("channelId").to_string();
        let Some(s) = sim_tags.get(&id) else {
            problems.push(format!("{id} {}: нет в симуляторе", tag.name));
            continue;
        };
        let protocol = tag.protocol.as_deref().unwrap_or("opcua");
        let expected_type = match tag.data_type.as_str() {
            "FLOAT" => "float",
            "INT32" => "int",
            "STRING" => "string",
            "BOOLEAN" => "bool",
            other => other,
        };
        if s.protocol != protocol {
            problems.push(format!("{id}: протокол {protocol} ≠ {}", s.protocol));
        }
        if s.kind != expected_type {
            problems.push(format!("{id}: тип {} ≠ {}", tag.data_type, s.kind));
        }
        if tag.writable != (s.access == "RW") {
            problems.push(format!("{id}: writable {} ≠ access {}", tag.writable, s.access));
        }
        if tag.device_name != s.device || tag.field_name != s.field {
            problems.push(format!(
                "{id}: прибор {:?}.{:?} ≠ {:?}.{:?}",
                tag.device_name, tag.field_name, s.device, s.field
            ));
        }
        match protocol {
            "opcua" if tag.node_id != format!("ns=2;s={id}") => problems.push(format!("{id}: nodeId {}", tag.node_id)),
            "modbus" => {
                if tag.modbus_address.map(|a| a - 40001) != s.modbus_address {
                    problems.push(format!("{id}: регистр {:?} ≠ {:?}", tag.modbus_address, s.modbus_address));
                }
                if tag.modbus_type != s.modbus_type {
                    problems.push(format!("{id}: modbusType {:?} ≠ {:?}", tag.modbus_type, s.modbus_type));
                }
            }
            _ => {}
        }
    }
    assert_eq!(seen, sim_tags.len(), "каналов в шлюзе {seen}, в симуляторе {}", sim_tags.len());
    assert!(problems.is_empty(), "{} расхождений: {:?}", problems.len(), &problems[..problems.len().min(15)]);
}
