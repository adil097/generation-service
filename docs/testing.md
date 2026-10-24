# Проверки конкурентности и восстановления

Матрица этапа 10 по [заданию](../TASK.md) и [архитектуре](architecture.md).
Тесты используют настоящую PostgreSQL и отдельные соединения, сессии и
транзакции. Базы `test_schema_<uuid>` создаются на время проверки и удаляются
фикстурой; рабочая база сервиса не очищается.

Полный набор запускается из корня проекта:

```powershell
docker compose --profile test run --rm --build tests
```

Для отдельных проверок используйте ту же конфигурацию БД:

```powershell
docker compose --profile test run --rm --build tests /app/.venv/bin/python -m pytest tests/test_concurrency.py tests/test_dispatch.py -v
```

## Четыре обязательных сценария

| Требование | Проверка | Проверяемый результат |
| --- | --- | --- |
| 20 одинаковых вебхуков оплаты одновременно | `test_concurrency.py::test_twenty_signed_payments_across_four_http_processes` | 20 подписанных HTTP-запросов проходят через четыре разных PID; одно начисление и одна строка журнала |
| 10 отказов последней ноги одновременно | `test_concurrency.py::test_ten_last_leg_failures_and_late_results_across_four_processes` | Одна финализация и один возврат; поздние успехи не меняют задачу и журнал |
| Отказ первой ноги, успех второй | `test_concurrency.py::test_fallback_and_late_old_result_with_four_live_executors[failure]` | История alpha → beta, результат beta, одно исходное списание, возврата нет |
| Поздний completed после сторожа | `test_concurrency.py::test_fallback_and_late_old_result_with_four_live_executors[watchdog]` | Четыре сторожа конкурируют за таймаут; старый успех игнорируется и при активной beta, и после её завершения |

`test_http_workflow.py` дополнительно проверяет полный цикл с автоматическими
подписанными вебхуками от обоих фейковых провайдеров: успех, fallback,
`payload`, модерацию, отказ обеих ног, отсутствие вебхука и поздний результат.

## Дополнительные сценарии

| Область | Тесты | Проверяемый результат |
| --- | --- | --- |
| Баланс на одну генерацию, последнее свободное место | `test_concurrency.py::test_competing_http_generations_respect_balance_and_last_slot`; `test_generation.py` | Из 20 конкурентных HTTP-запросов проходит один, остальные получают 402 либо 429; отрицательного баланса и четвёртой активной задачи нет |
| Конфликт оплаты | `test_finance.py::test_same_payment_id_racing_for_different_users`; `test_api.py::test_payment_identifier_cannot_credit_a_different_user` | Уникальный payment_id не начисляется другому пользователю или с другой суммой |
| Гонка успеха, отказа и сторожей | `test_concurrency.py::test_provider_results_race_four_watchdogs_without_result_and_refund`; `test_attempts.py::test_success_failure_and_watchdog_race_has_one_winner` | Только одно завершение; результат и возврат не сосуществуют. Перед стартом гонки наблюдается реальное ожидание блокировки PostgreSQL |
| Модерация, payload и число референсов | `test_attempts.py::test_routing_moderation_payload_and_references`, `test_moderation_skips_all_flagged_legs_without_history`, `test_latest_actual_failure_controls_moderation_filter`; `test_generation.py` | Следующая нога выбирается по фактическому отказу и референсам; payload завершает задачу сразу; пропущенные ноги не создают историю |
| Ранние, повторные и недостоверные события | `test_api.py`, `test_attempts.py`, `test_dispatch.py::test_early_webhook_wins_over_late_202_and_failure` | Подпись и корреляция проверяются до применения; ранний результат допустим, поздний 202 не переписывает завершение |
| Отправка и токены | `test_dispatch.py::test_twenty_claims_and_acknowledgements_have_one_owner`, `test_recovery_preserves_identity_payload_and_original_deadline`, `test_stale_or_expired_claim_does_not_start_http` | Один актуальный владелец; старые токены не сохраняют ответ или ошибку; запрос и общий дедлайн не меняются |
| SKIP LOCKED и поиск по страницам | `test_dispatch.py::test_dispatch_skip_locked_and_pagination_do_not_starve_other_work`, `test_watchdog_pagination_skips_locked_user_and_continues`; `test_attempts.py::test_watchdog_skip_locked_defers_and_releases_partial_locks` | Занятая работа не блокирует остальные задачи и не теряется |
| HTTP-ошибки и ошибки БД | `test_dispatch.py`, `test_api.py` | 5xx и таймауты приводят к fallback; ошибки БД не становятся отказом провайдера; неизвестный COMMIT создания не переигрывается автоматически |
| Согласованное чтение | `test_finance.py::test_balance_and_history_share_snapshot`; `test_api.py::test_task_and_history_share_repeatable_read_snapshot` | Баланс и журнал, задача и попытки читаются из одного снимка |
| Несколько исполнителей провайдера | `test_provider_tasks.py::test_acceptance_from_four_processes_has_one_generation`, `test_concurrent_claims_and_stale_expired_tokens` | Идемпотентность и токены доставки обеспечиваются БД, а не памятью процесса |

## Принудительное завершение процессов

В `test_concurrency.py` поднимаются четыре независимых API-процесса со своими
пулами БД и фоновыми отправителями/сторожами. Оба фейковых провайдера работают
через реальные HTTP-серверы; автоматическая доставка их вебхуков отключена,
чтобы тест точно управлял входящими событиями. PID в ответах добавляется только
тестовой обёрткой и подтверждает участие всех четырёх процессов.

`tests/http_worker.py` умеет остановить конкретный HTTP-запрос после выполнения
SQL до COMMIT либо после успешного COMMIT до отправки ответа. Родитель получает
сигнал по межпроцессному каналу и вызывает `Process.kill()`. Соединения и
транзакции не закрываются штатным finally. Три оставшихся процесса продолжают
работать с той же БД. Эти точки остановки находятся только в тестах.

| Точка падения | Проверка | Восстановление |
| --- | --- | --- |
| Начисление оплаты до/после COMMIT | `test_killed_http_process_is_recovered_by_three_survivors[payment-*]` | До COMMIT повтор применяет оплату один раз; после COMMIT все повторы обнаруживают прежнее начисление |
| Создание задачи до/после COMMIT | Тот же тест, `generation-*` | До COMMIT нет задачи, попытки и списания; после COMMIT выжившие отправители находят задачу без повторного POST /generate |
| Переход на следующую ногу до/после COMMIT | Тот же тест, `fallback-*` | Повтор создаёт только одну следующую попытку либо видит уже сохранённую; beta выполняется без второго списания |
| Возврат до/после COMMIT | Тот же тест, `refund-*` | Изменения статусов и возврат атомарны; повтор даёт ровно один возврат |
| Успешное завершение до/после COMMIT | Тот же тест, `completion-*` | После повторов остаются один результат, одно списание и ни одного возврата |
| Захват отправки, до HTTP | `test_dispatch.py::test_abrupt_exit_at_dispatch_commits_preserves_deadline_and_work[claim]` | После истечения токена четыре новых исполнителя отправляют сохранённый запрос один раз |
| Провайдер принял задачу, 202 не сохранён | `test_dispatch.py::test_process_crash_after_provider_commit_recovers_one_external_generation` | Идемпотентный повтор возвращает прежний external_id, новой генерации и списания нет |
| Сохранение 202 | `test_dispatch.py::test_abrupt_exit_at_dispatch_commits_preserves_deadline_and_work[accepted]` | awaiting_result переживает падение, повторной отправки нет; сторожа могут завершить ожидание |
| Повторные падения после захвата | Тот же тест, `repeated_claim` | Три аварийных выхода не меняют исходный дедлайн и запрос; сторожа закрывают pending с одним возвратом |
| Провайдер отправил вебхук, подтверждение не сохранено | `test_provider_tasks.py::test_process_crash_after_send_recovers_exact_callback` | После падения повторяются те же подписанные байты |

Тесты отправки используют `os._exit()` в дочернем процессе после фактической
фиксации транзакции, также без очистки. Истечение сроков моделируется изменением
только временных отметок в изолированной тестовой БД: проверки не ждут 60 секунд
и не зависят от выбора случайной задержки провайдера.

## Проверка чистого запуска

Проверка этапа 11 использует отдельное имя Compose-проекта, новые порты и новый
том PostgreSQL. Перед запуском убедитесь, что выбранное имя ещё не используется:

```powershell
docker ps -a --filter label=com.docker.compose.project=task-job-clean
docker volume ls --filter label=com.docker.compose.project=task-job-clean
Copy-Item .env.example .env.clean-check
```

Оба списка должны быть пустыми; иначе выберите другое имя во всех командах.
В `.env.clean-check` измените `API_PORT=18000`, `POSTGRES_PORT=55433` и порт
в `DATABASE_URL` на `55433`. Эти порты должны быть свободны. Файл исключён
из Git правилом `.env.*`. Во всех командах ниже используются явные `-p`
и `--env-file`, чтобы не затронуть обычный проект `task-job`.

```powershell
docker compose -p task-job-clean --env-file .env.clean-check build --no-cache api alpha beta db-bootstrap migrate seed tests
docker compose -p task-job-clean --env-file .env.clean-check up -d --wait
docker compose -p task-job-clean --env-file .env.clean-check ps -a
docker compose -p task-job-clean --env-file .env.clean-check --profile tools run --rm --no-deps seed
docker compose -p task-job-clean --env-file .env.clean-check --profile tools run --rm --no-deps seed
docker compose -p task-job-clean --env-file .env.clean-check run --rm --no-deps migrate alembic current
docker compose -p task-job-clean --env-file .env.clean-check run --rm --no-deps migrate alembic check
docker compose -p task-job-clean --env-file .env.clean-check --profile test run --rm --no-deps tests
Invoke-RestMethod http://localhost:18000/health/ready
```

Ожидается: четыре основных сервиса healthy, `db-bootstrap` и `migrate`
завершились с кодом 0, версия БД `0002 (head)`, расхождений схемы нет,
все 350 тестов проходят без пропусков. Двойной seed оставляет начальные
балансы 100 и 50, по одному начислению на пользователя.

Выполните примеры генерации и подписанной оплаты из README для этого проекта:
замените `$baseUrl` на `http://localhost:18000`, а в команде оплаты используйте
`docker compose -p task-job-clean --env-file .env.clean-check exec -T api python -`.
Генерация должна завершиться с балансом 98. Доставка оплаты на 10 токенов дважды
даёт `applied=true`, затем `applied=false`, итоговый баланс 108.

Затем проверьте сохранность состояния:

```powershell
docker compose -p task-job-clean --env-file .env.clean-check down
docker compose -p task-job-clean --env-file .env.clean-check up -d --wait
```

Та же задача должна остаться `completed` с прежним результатом, баланс — 108.
После проверки удалите только созданное проверочное окружение:

```powershell
docker compose -p task-job-clean --env-file .env.clean-check down --volumes
Remove-Item -LiteralPath .env.clean-check
```

24 октября 2026 года этот путь проверен с новым томом и сборкой без кеша:
350 тестов прошли, seed идемпотентен, генерация и подписанная оплата работают,
данные пережили пересоздание контейнеров. Также проверены генерация пустой
ревизии Alembic во временной копии migrations, её применение и откат к 0002.
Обычный проект и его данные сохранены; проверочные Docker-контейнеры,
сеть и том удалены.
