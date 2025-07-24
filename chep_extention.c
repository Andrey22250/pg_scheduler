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

/* Главная функция фонового воркера */
void scheduler_main(Datum arg)
{
    int rc;

    /* Установка обработчика сигналов */
    pqsignal(SIGTERM, scheduler_die);
    BackgroundWorkerUnblockSignals();

    /* Подключение к целевой базе данных */
    BackgroundWorkerInitializeConnection(scheduler_database, NULL, 0);

    elog(LOG, "[scheduler] Воркер запущен, wake_interval = %ld мкс", scheduler_sleep_us);

    while (!got_sigterm)
    {
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       scheduler_sleep_us / 1000,
                       PG_WAIT_EXTENSION);

        /* Проверка завершения postmaster'а */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(MyLatch);

        PG_TRY();
        {
            int spi_ret;
            bool isnull;
            uint64 i;
            uint64 jobs_executed = 0;

            const char *query_jobs =
                "SELECT job_id FROM scheduler.jobs "
                "WHERE enabled AND next_run <= now() "
                "ORDER BY next_run "
                "FOR UPDATE SKIP LOCKED";

            if (SPI_connect() != SPI_OK_CONNECT)
            {
                elog(WARNING, "[scheduler] SPI_connect() не удался");
                return;
            }

            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());

            spi_ret = SPI_execute(query_jobs, false, 0);
            if (spi_ret == SPI_OK_SELECT && SPI_processed > 0)
            {
                for (i = 0; i < SPI_processed; i++)
                {
                    int32 job_id;
                    char query_exec[128];

                    job_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                                         SPI_tuptable->tupdesc,
                                                         1,
                                                         &isnull));
                    if (isnull)
                        continue;

                    snprintf(query_exec, sizeof(query_exec),
                             "SELECT scheduler.execute_job(%d);", job_id);

                    PG_TRY();
                    {
                        SPI_execute(query_exec, false, 0);
                        jobs_executed++;
                    }
                    PG_CATCH();
                    {
                        EmitErrorReport();
                        FlushErrorState();
                        elog(WARNING, "[scheduler] Ошибка выполнения задания job_id=%d", job_id);
                    }
                    PG_END_TRY();
                }
            }

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();

            if (jobs_executed > 0)
                elog(DEBUG1, "[scheduler] Выполнено %lu заданий", jobs_executed);
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
            AbortCurrentTransaction();
            SPI_finish();
        }
        PG_END_TRY();
    }

    elog(LOG, "[scheduler] Завершение работы воркера");
    proc_exit(0);
}