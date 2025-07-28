//chep_extention - это мой первый скедулер для PostgreSQL

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "limits.h"
#include "string.h"

PG_MODULE_MAGIC;

/* Настраиваемые параметры */
static char *scheduler_database = NULL;
static char *scheduler_wake_interval = NULL;
static int64 scheduler_sleep_us = 10 * 1000000L; // по умолчанию 10 секунд

/* Прототипы */
void _PG_init(void);
void scheduler_main(Datum);
static int64 parse_wake_interval(const char *s);

/* Инициализация расширения */
/**
 * _PG_init() — точка входа при инициализации модуля
 *
 * Вызывается PostgreSQL при запуске, если расширение указано в
 * shared_preload_libraries. Здесь регистрируются GUC-параметры
 * и фоновый воркер, который будет периодически опрашивать таблицу scheduler.jobs.
 */
void _PG_init(void)
{
    BackgroundWorker worker;

    /* Проверка: загружено ли через shared_preload_libraries */
    if (!process_shared_preload_libraries_in_progress)
    {
        elog(WARNING, "[scheduler] Must be loaded via shared_preload_libraries");
        return;
    }

    /* Регистрируем GUC-параметр: интервал между проверками заданий */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Polling interval for scheduler (e.g., '10s', '2 min', '1h')",
        "Supports time units: s, min, h, d, w, mon.",
        &scheduler_wake_interval,
        "10s",
        PGC_POSTMASTER,  // Должно быть установлено при старте сервера
        0,
        NULL, NULL, NULL
    );

    /* Регистрируем GUC-параметр: имя базы данных с таблицей scheduler.jobs */
    DefineCustomStringVariable(
        "scheduler.database",
        "Database that contains the scheduler schema and jobs",
        "This database must include the 'scheduler.jobs' table.",
        &scheduler_database,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    /* Преобразуем интервал сна в микросекунды */
    scheduler_sleep_us = parse_wake_interval(scheduler_wake_interval);

    /* Обнуляем структуру фонового воркера */
    MemSet(&worker, 0, sizeof(BackgroundWorker));

     /* Задаём имя процесса, которое будет видно в списке процессов PostgreSQL */
    strlcpy(worker.bgw_name, "PostgreSQL Scheduler by Staryi", BGW_MAXLEN);
    strlcpy(worker.bgw_type, "scheduler by Staryi", BGW_MAXLEN);
    strlcpy(worker.bgw_library_name, "chep_extention", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    /* Флаги: доступ к shared memory и соединение с БД */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    /* При сбое перезапускать через 5 секунд */
    worker.bgw_restart_time = 5;
    /* Указатель на основную функцию воркера */
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    /* Регистрация рабочего процесса */
    RegisterBackgroundWorker(&worker);
    elog(LOG, "[scheduler] Background worker registered successfully");
}


/* Функция разбора интервала */
static int64 parse_wake_interval(const char *s)
{
    double value;
    char unit[8] = "";
    int64 result;
    char *endptr = NULL;
    int i = 0;

    if (s == NULL || *s == '\0')
        return 10 * 1000000L;

    while (isspace((unsigned char)*s)) s++;

    errno = 0;
    value = strtod(s, &endptr);
    if (errno != 0 || endptr == s || value < 0)
        goto fail;

    while (isspace((unsigned char)*endptr)) endptr++;

    while (i < 7 && endptr[i] && isalpha((unsigned char)endptr[i])) {
        unit[i] = tolower((unsigned char)endptr[i]);
        i++;
    }
    unit[i] = '\0';

    if (strcmp(unit, "s") == 0 || strcmp(unit, "sec") == 0)
        result = (int64)(value * 1000000.0L);
    else if (strcmp(unit, "min") == 0)
        result = (int64)(value * 60.0L * 1000000.0L);
    else if (strcmp(unit, "h") == 0 || strcmp(unit, "hour") == 0)
        result = (int64)(value * 3600.0L * 1000000.0L);
    else if (strcmp(unit, "d") == 0 || strcmp(unit, "day") == 0)
        result = (int64)(value * 86400.0L * 1000000.0L);
    else
        result = (int64)(value * 1000000.0L);  // Изначально считаем в микросекундах

    return result;

fail:
    elog(WARNING, "Invalid wake interval: \"%s\", defaulting to 10s", s);
    return 10 * 1000000L;
}

/* Обработчик SIGTERM */
static volatile sig_atomic_t got_sigterm = false;
static void scheduler_die(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Безопасное выполнение одного задания */
static void execute_job_safely(int32 job_id)
{
    char query_exec[128];
    int ret;

    snprintf(query_exec, sizeof(query_exec),
             "SELECT scheduler.execute_job(%d);", job_id);

    elog(LOG, "[execute] Запуск задания job_id = %d", job_id);

    PG_TRY();
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            elog(WARNING, "[scheduler] SPI_connect() не удался в execute_job_safely()");
        }

        ret = SPI_execute(query_exec, false, 0);
        if (ret != SPI_OK_SELECT)
            elog(WARNING, "[scheduler] scheduler.execute_job(%d) вернул код %d", job_id, ret);

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        EmitErrorReport();
        FlushErrorState();
        AbortCurrentTransaction();
    }
    PG_END_TRY();
}

PGDLLEXPORT void scheduler_main(Datum arg);

/* Главная функция фонового воркера */
void scheduler_main(Datum arg)
{
    int rc;

    pqsignal(SIGTERM, scheduler_die);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(scheduler_database, NULL, 0);

    elog(LOG, "[scheduler] Воркер запущен, wake_interval = %ld мкс", scheduler_sleep_us);

    while (!got_sigterm)
    {
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       scheduler_sleep_us / 1000,
                       PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH)
        {
            elog(LOG, "[scheduler] Обнаружена остановка postmaster, завершаем работу.");
            proc_exit(1);
        }

        ResetLatch(MyLatch);

        // Объявляем переменные здесь для видимости в PG_CATCH
        int32 *job_ids = NULL;
        uint64 job_count = 0;

        PG_TRY();
        {
            const char *query_jobs =
                "SELECT job_id FROM scheduler.jobs "
                "WHERE enabled AND next_run <= now() "         
                "FOR UPDATE SKIP LOCKED";

            int spi_ret;
            uint64 jobs_executed = 0;
            bool isnull;

            // Запоминаем контекст до подключения SPI (он переживет SPI_finish)
            MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());

            if (SPI_connect() != SPI_OK_CONNECT)
                elog(ERROR, "[scheduler] SPI_connect() не удался");
            elog(LOG, "[scheduler] Ищем задания для выполнения...");
            spi_ret = SPI_execute(query_jobs, false, 0);
            if (spi_ret != SPI_OK_SELECT)
                elog(ERROR, "[scheduler] Ошибка SPI_execute: код %d", spi_ret);

            job_count = SPI_processed;

            if (job_count > 0)
            {
                elog(LOG, "[scheduler] Найдено заданий: %ld", job_count);
                // ВАЖНО: выделяем в старом контексте (не SPI)
                job_ids = (int32 *) MemoryContextAlloc(oldcontext, sizeof(int32) * job_count);

                for (uint64 i = 0; i < job_count; i++)
                {
                    Datum job_datum = SPI_getbinval(SPI_tuptable->vals[i],
                                                    SPI_tuptable->tupdesc,
                                                    1,
                                                    &isnull);
                    if (isnull)
                    {
                        elog(WARNING, "[scheduler] NULL job_id — пропущено");
                        job_ids[i] = -1;
                    }
                    else
                    {
                        job_ids[i] = DatumGetInt32(job_datum);
                        elog(LOG, "[scheduler] Номер задания: %d", job_ids[i]);
                    }
                }
            }

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();

            // Выполняем задания по сохранённому списку
            for (uint64 i = 0; i < job_count; i++)
            {
                if (job_ids[i] >= 0)
                {
                    elog(LOG, "[scheduler] Номер задания: %d", job_ids[i]);
                    execute_job_safely(job_ids[i]);
                    jobs_executed++;
                }
            }

            if (job_ids)
                pfree(job_ids);

            elog(LOG, "[scheduler] Цикл завершён, выполнено заданий: %lu", jobs_executed);
        }
        PG_CATCH();
        {
            elog(LOG, "[scheduler] Цикл завершён, выполнено заданий: ");
            // Освобождаем память при ошибке
            if (job_ids)
                pfree(job_ids);
            job_ids = NULL;

            EmitErrorReport();
            FlushErrorState();
            AbortCurrentTransaction();  // Откатываем транзакцию при ошибке
        }
        PG_END_TRY();
    }

    elog(LOG, "[scheduler] Завершение работы воркера");
    proc_exit(0);
}