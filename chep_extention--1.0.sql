CREATE SCHEMA IF NOT EXISTS scheduler;

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

-- Функция расчёта next_run
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
          current_attempts = 0,
          last_run = NOW(),
          next_run = scheduler.calculate_next_run(EXCLUDED.schedule_spec, NOW());
END;
$$ LANGUAGE plpgsql;

-- Enable or disable a job by name
CREATE OR REPLACE FUNCTION scheduler.toggle_job(
    p_name TEXT,
    p_enabled BOOLEAN
) RETURNS VOID AS $$
BEGIN
    UPDATE scheduler.jobs
       SET enabled = p_enabled
     WHERE job_name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', p_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Delete a job and its logs
CREATE OR REPLACE FUNCTION scheduler.delete_job(
    p_name TEXT
) RETURNS VOID AS $$
BEGIN
    DELETE FROM scheduler.jobs WHERE job_name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', p_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Функция вычисления следующего времени запуска на основе cron-выражения
CREATE OR REPLACE FUNCTION scheduler.cron_next_run(
    cron_expr TEXT,
    base_time TIMESTAMPTZ
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    parts TEXT[];         -- массив из частей cron-выражения
    cron_min TEXT;        -- минуты
    cron_hour TEXT;       -- часы
    cron_dom TEXT;        -- день месяца
    cron_month TEXT;      -- месяц
    cron_dow TEXT;        -- день недели
    ts TIMESTAMPTZ;       -- временная метка для проверки
BEGIN
    -- Разбиваем выражение на 5 частей: min hour day month dow
    parts := string_to_array(cron_expr, ' ');
    IF array_length(parts,1) <> 5 THEN
        RAISE EXCEPTION 'Неверное cron-выражение: %', cron_expr;
    END IF;
    cron_min   := parts[1];
    cron_hour  := parts[2];
    cron_dom   := parts[3];
    cron_month := parts[4];
    cron_dow   := parts[5];

    -- Итерируемся по каждой минуте от base_time +1мин до base_time+1месяц
    FOR ts IN SELECT generate_series(
                    date_trunc('minute', base_time) + interval '1 minute',
                    base_time + interval '1 month',
                    interval '1 minute')
    LOOP
        -- Проверяем соответствие каждого компонента выражению или wildcard '*'
        IF (cron_min = '*' OR to_char(ts, 'MI') = lpad(cron_min,2,'0'))
        AND (cron_hour = '*' OR to_char(ts, 'HH24') = lpad(cron_hour,2,'0'))
        AND (cron_dom = '*' OR to_char(ts, 'DD') = lpad(cron_dom,2,'0'))
        AND (cron_month = '*' OR to_char(ts, 'MM') = lpad(cron_month,2,'0'))
        AND (cron_dow = '*' OR to_char(ts, 'D') = cron_dow)
        THEN
            RETURN ts;  -- найден подходящий момент
        END IF;
    END LOOP;

    -- Если не найдено, выбрасываем ошибку
    RAISE EXCEPTION 'Не удалось найти следующий запуск для % после %', cron_expr, base_time;
END;
$$ LANGUAGE plpgsql;

-- Execute a job by ID, log results and compute next_run
CREATE OR REPLACE FUNCTION scheduler.execute_job(
    p_job_id INT
) RETURNS VOID AS $$
DECLARE
    rec RECORD;
    start_ts TIMESTAMPTZ;
    duration INTERVAL;
    status TEXT;
    msg TEXT;
BEGIN
    -- Lock job row
    SELECT * INTO rec
      FROM scheduler.jobs
     WHERE job_id = p_job_id;

    start_ts := clock_timestamp();

    -- Execute command
    BEGIN
        IF rec.job_type = 'sql' THEN
            EXECUTE rec.command;
        ELSIF rec.job_type = 'shell' THEN
            -- Выполнение shell-команды через COPY PROGRAM
            EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', rec.command);
        ELSE
            RAISE EXCEPTION 'Unknown job_type: %', rec.job_type;
        END IF;
        status := 'success';
        msg := '';
    EXCEPTION WHEN OTHERS THEN
        status := 'failure';
        msg := SQLERRM;
    END;

    duration := clock_timestamp() - start_ts;

    -- Insert into logs
    INSERT INTO scheduler.job_logs(job_id, run_time, status, message, duration)
    VALUES (p_job_id, start_ts, status, msg, duration);

    -- Update job metadata
    UPDATE scheduler.jobs
       SET last_run = start_ts,
           last_status = status,
           last_message = msg,
           current_attempts = CASE WHEN status = 'failure' THEN rec.current_attempts + 1 ELSE 0 END,
           enabled = CASE
                         WHEN status = 'failure' AND rec.current_attempts + 1 >= rec.max_attempts THEN FALSE
                         ELSE TRUE
                     END,
           next_run = CASE
                          WHEN enabled THEN scheduler.calculate_next_run(rec.schedule_spec, start_ts)
                          ELSE NULL
                      END
     WHERE job_id = p_job_id;
END;
$$ LANGUAGE plpgsql;