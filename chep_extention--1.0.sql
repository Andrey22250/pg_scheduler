-- Указание версии расширения
CREATE EXTENSION IF NOT EXISTS plpgsql;

CREATE SCHEMA IF NOT EXISTS chep_scheduler;

-- Таблица заданий
CREATE TABLE scheduler.jobs (
    job_id        SERIAL PRIMARY KEY,
    job_name      TEXT UNIQUE NOT NULL,
    job_type      TEXT NOT NULL CHECK (job_type IN ('sql','shell')),
    command       TEXT NOT NULL,
    schedule_spec TEXT NOT NULL, -- строка с описанием расписания: 'once at TIMESTAMP', 'interval 2 hours', 'cron 55 23 * * *'
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    last_run      TIMESTAMPTZ,
    next_run      TIMESTAMPTZ,
    max_attempts  INT NOT NULL DEFAULT 1,
    current_attempts INT NOT NULL DEFAULT 0,
    last_status   TEXT,
    last_message  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Таблица логов
CREATE TABLE scheduler.job_logs (
    log_id    SERIAL PRIMARY KEY,
    job_id    INT REFERENCES scheduler.jobs(job_id) ON DELETE CASCADE,
    run_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status    TEXT,
    message   TEXT,
    duration  INTERVAL
);

-- Триггер для updated_at
CREATE OR REPLACE FUNCTION scheduler.update_timestamp() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_jobs_updated
    BEFORE UPDATE ON scheduler.jobs
    FOR EACH ROW EXECUTE FUNCTION scheduler.update_timestamp();

--- Функция расчёта next_run
-- (парсер schedule_spec: разбор cron/interval/once)
CREATE OR REPLACE FUNCTION scheduler.calculate_next_run(spec TEXT, last TIMESTAMPTZ) RETURNS TIMESTAMPTZ AS $$
DECLARE
    interval_prefix TEXT := 'interval ';
    once_prefix TEXT := 'once at ';
    cron_prefix TEXT := 'cron ';
    interval_text TEXT;
    once_time TIMESTAMPTZ;
    cron_expr TEXT;
BEGIN
    IF spec ILIKE interval_prefix || '%' THEN
        interval_text := TRIM(BOTH ' ' FROM SUBSTRING(spec FROM LENGTH(interval_prefix)+1));
        RETURN last + interval_text::interval;

    ELSIF spec ILIKE once_prefix || '%' THEN
        once_time := TRIM(BOTH ' ' FROM SUBSTRING(spec FROM LENGTH(once_prefix)+1))::timestamptz;
        RETURN once_time;

    ELSIF spec ILIKE cron_prefix || '%' THEN
        -- Для упрощения: вызываем вспомогательную SQL-функцию (например, на основе pg_cron или pg_cron_next)
        cron_expr := TRIM(BOTH ' ' FROM SUBSTRING(spec FROM LENGTH(cron_prefix)+1));
        RETURN scheduler.cron_next_run(cron_expr, last);

    ELSE
        RAISE EXCEPTION 'Unknown schedule_spec format: %', spec;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Триггер для установки next_run при INSERT/UPDATE
CREATE OR REPLACE FUNCTION scheduler.set_next_run() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.enabled THEN
        NEW.next_run = scheduler.calculate_next_run(NEW.schedule_spec, COALESCE(NEW.last_run, NOW()));
    ELSE
        NEW.next_run = NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_set_next_run
    BEFORE INSERT OR UPDATE ON scheduler.jobs
    FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

-- API: добавление/обновление задания
CREATE OR REPLACE FUNCTION scheduler.add_job(
    p_name TEXT,
    p_type TEXT,
    p_cmd  TEXT,
    p_spec TEXT,
    p_max_attempts INT DEFAULT 1
) RETURNS VOID AS $$
BEGIN
    INSERT INTO scheduler.jobs (job_name, job_type, command, schedule_spec, max_attempts)
    VALUES (p_name, p_type, p_cmd, p_spec, p_max_attempts)
    ON CONFLICT (job_name) DO UPDATE
      SET command = EXCLUDED.command,
          schedule_spec = EXCLUDED.schedule_spec,
          max_attempts = EXCLUDED.max_attempts,
          enabled = TRUE,
          current_attempts = 0;
END;
$$ LANGUAGE plpgsql;