-- Фильтр локальной истории: переопределения gateway.history.* для отдельных тегов (блок history: в
-- controllers.yaml). NULL — взять умолчание. Та же схема, что у Java-шлюза (Flyway V2): IF NOT EXISTS —
-- Rust-шлюз встаёт и на базу, где миграцию уже применила Java-версия.
ALTER TABLE tags ADD COLUMN IF NOT EXISTS history_deadband double precision;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS history_deadband_percent double precision;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS history_min_interval_ms bigint;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS history_max_interval_ms bigint;

-- История читается по тегу за интервал и чистится по времени — без индексов оба запроса читают всю таблицу.
CREATE INDEX IF NOT EXISTS telemetry_tag_time_idx ON public.telemetry (tag_id, "time");
CREATE INDEX IF NOT EXISTS telemetry_time_idx ON public.telemetry ("time");
