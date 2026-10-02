//! SCADA Gateway на Rust: опрос контроллеров (OPC UA, Modbus TCP, PAC driver-master) →
//! Kafka для монитора; приём команд записи; журнал событий в PostgreSQL.
//!
//! Библиотека — чтобы модули были доступны интеграционным тестам (`tests/`); точка входа
//! процесса — `main.rs`.

pub mod app;
pub mod command;
pub mod config;
pub mod db;
pub mod events;
pub mod filter;
pub mod ha;
pub mod http;
pub mod kafka;
pub mod leadership;
pub mod messages;
pub mod metrics;
pub mod modbus;
pub mod model;
pub mod opcua;
pub mod pac;
pub mod poller;
pub mod sandbox;
pub mod script;
pub mod telemetry;
