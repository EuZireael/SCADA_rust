-- Схема БД шлюза — та же, что у Java-шлюза (Flyway V1__baseline_schema.sql). Всё через
-- IF NOT EXISTS: Rust-шлюз встаёт и на пустую базу, и на scada_db, созданную Java-версией.

CREATE TABLE IF NOT EXISTS public.controllers (
    id bigserial PRIMARY KEY,
    created_at timestamp(6) without time zone,
    description character varying(255),
    enabled boolean,
    endpoint character varying(255) NOT NULL,
    name character varying(255) NOT NULL UNIQUE,
    password character varying(255),
    security_policy character varying(255),
    updated_at timestamp(6) without time zone,
    username character varying(255)
);

CREATE TABLE IF NOT EXISTS public.tags (
    id bigserial PRIMARY KEY,
    channel_id bigint,
    created_at timestamp(6) without time zone,
    data_type character varying(255) NOT NULL,
    description character varying(255),
    device_name character varying(255),
    device_type character varying(255),
    enabled boolean,
    field_name character varying(255),
    fields_json text,
    max_value double precision,
    min_value double precision,
    modbus_address integer,
    modbus_type character varying(255),
    modbus_unit_id integer,
    name character varying(255) NOT NULL,
    node_id character varying(255) NOT NULL,
    polling_rate bigint,
    protocol character varying(255) NOT NULL,
    record_device boolean,
    unit character varying(255),
    updated_at timestamp(6) without time zone,
    writable boolean,
    controller_id bigint NOT NULL REFERENCES public.controllers(id)
);

CREATE TABLE IF NOT EXISTS public.event_log (
    id bigserial PRIMARY KEY,
    acknowledged boolean,
    controller_id bigint,
    details text,
    event_time timestamp(6) with time zone NOT NULL,
    event_type character varying(50) NOT NULL,
    message character varying(500),
    severity character varying(20),
    source character varying(100),
    tag_id bigint,
    user_id character varying(255)
);

CREATE TABLE IF NOT EXISTS public.telemetry (
    id bigserial PRIMARY KEY,
    quality character varying(20),
    raw_data bytea,
    tag_id bigint NOT NULL,
    "time" timestamp(6) with time zone NOT NULL,
    value double precision,
    value_str character varying(255)
);

-- Индексы, которых у Java-схемы не было: выборки истории по тегу и журнала по времени
-- без них — полный скан таблиц с миллионами строк.
CREATE INDEX IF NOT EXISTS telemetry_tag_time_idx ON public.telemetry (tag_id, "time");
CREATE INDEX IF NOT EXISTS telemetry_time_idx ON public.telemetry ("time");
CREATE INDEX IF NOT EXISTS event_log_time_idx ON public.event_log (event_time DESC);
CREATE INDEX IF NOT EXISTS tags_controller_idx ON public.tags (controller_id);
