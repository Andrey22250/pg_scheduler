-- Очистка предыдущих данных
DELETE FROM scheduler.job_logs;
DELETE FROM scheduler.jobs;

-- 1. Тест: добавление задания
SELECT scheduler.add_job(
  'test_sql', 
  'sql', 
  $$SELECT 1$$, 
  'interval 1 minute', 
  2
);

-- Ожидается: задание создано с именем 'test_sql'
SELECT job_name, job_type, schedule_spec, max_attempts
  FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 2. Тест: обновление существующего задания
SELECT scheduler.add_job(
  'test_sql', 
  'sql', 
  $$SELECT 42$$, 
  'interval 2 minutes', 
  4
);

-- Ожидается: задание обновлено
SELECT command, schedule_spec, max_attempts
  FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 3. Тест: выключение и включение задания
SELECT scheduler.toggle_job('test_sql', false);
SELECT enabled FROM scheduler.jobs WHERE job_name = 'test_sql';

SELECT scheduler.toggle_job('test_sql', true);
SELECT enabled FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 4. Тест: удаление задания
SELECT scheduler.delete_job('test_sql');
-- Ожидается: 0 строк
SELECT * FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 5. Тест: выполнение shell-задания
-- (Создаётся shell-задача, исполняется вручную, проверяется лог)

-- Добавление shell-задачи (команда: просто echo)
SELECT scheduler.add_job(
  'test_shell',
  'shell',
  'echo hello >> /tmp/chep_extention_test.log',
  'once at ' || to_char(now() + interval '10 seconds', 'YYYY-MM-DD HH24:MI:SS'),
  1
);

-- Принудительное выполнение
SELECT scheduler.execute_job(
  (SELECT job_id FROM scheduler.jobs WHERE job_name = 'test_shell')
);

-- Проверка логов
SELECT status, message, duration FROM scheduler.job_logs
  WHERE job_id = (SELECT job_id FROM scheduler.jobs WHERE job_name = 'test_shell');

-- 6. Очистка
SELECT scheduler.delete_job('test_shell');
