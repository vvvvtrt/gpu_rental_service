# Каталог событий – Сервис аренды GPU для запуска LLM

Тема 8. Проект: «Сервис аренды GPU для запуска на нём LLM».
Сервисы (bounded contexts): **Users**, **Inventory**, **Rental** (аренды,
hold, waitlist), **Billing** (баланс, платежи, usage), **Workloads**
(workspace, деплой моделей, jobs), **Admin** (поддержка, квоты, абуз).
Шина событий: каждый сервис кладёт событие в **outbox своей БД** (та же
транзакция, что изменение данных), relay публикует в брокер; hold и очередь
ожидания – в Redis (TTL hold), Saga между Rental и Billing.

## Соглашения

- **Ключ маршрутизации**: `«контекст».«сущность».«действие>`,
  exchange `«контекст».events` (topic), durable-очереди, persistent-сообщения,
  publisher confirms.
- **event_id**: UUID, генерируется один раз при записи в outbox, не меняется
  при повторах relay и возврате из DLQ – ключ дедупликации получателя.
- **Версия схемы**: `schema_version` в заголовке, текущая `v1`. Ломающие
  изменения – новый ключ `...v2` + expand–contract.
- **Версия сущности**: монотонный `aggregate_version` внутри агрегата
  (user, node, rental, payment, offer, workspace, deploy, job) – для
  оптимистичной защиты порядка и надгробий состояний. Для чисто
  фактических потоков (auth, usage) версия не ведётся.
- **Семантика доставки везде at-least-once**: ровно-один-раз недостижим
  (relay может упасть после publish до отметки `sent_at`), поэтому каждый
  получатель идемпотентен: inbox/processed по `event_id`.
- Время в заголовках – целые миллисекунды эпохи.

## 1. Управление пользователями (владелец – Users)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| UserRegistered | `users.user.registered` | v1 / user_version=1 | outbox + relay | Billing (открыть ledger), уведомления, аналитика | внутри user_id: до RoleChanged | inbox; Billing создаёт баланс не более одного раза (UNIQUE user_id в ledger) | временные → retry ≤3, backoff+jitter → DLQ; постоянные (схема) → сразу DLQ | DLQ depth, возраст outbox |
| UserAuthenticated | `users.user.authenticated` | v1 / – | outbox + relay | security-мониторинг (аномалии входов), audit | нет | inbox; повтор не плодит сессию дважды (сессия по session_id) | постоянных почти нет; 5xx подписчика → retry → DLQ | всплеск auth-failures по пользователю |
| UserUpdated | `users.user.updated` | v1 / user_version++ | outbox + relay | уведомления, сервисы с кешем профиля | внутри user_id относительно Registered | inbox; обработчик применяет по user_version, старую версию отбрасывает | как выше | lag обработки user_version |
| UserRoleChanged | `users.user.role_changed` | v1 / user_version++ | outbox + relay | API Gateway (инвалидация кеша ролей), Rental, Workloads, Billing | да: после Registered того же user_id | inbox + user_version; повтор не откатывает роль (применение по версии) | как выше | рассинхрон ролей Gateway vs Users |
| UserBanned | `users.user.banned` | v1 / user_version++ | outbox + relay | Rental (остановить аренды), Workloads (cancel jobs), Billing (заморозить списания), Gateway | да: после Registered | inbox + user_version; повтор повторно не роняет уже остановленное (состояние аренды проверяется) | как выше; бизнес-отказ «аренды нет» – не ошибка, ack | число активных аренд у забаненного > 0 спустя минуту |
| UserUnbanned | `users.user.unbanned` | v1 / user_version++ | outbox + relay | те же (восстановление) | да: после Banned того же user_id | inbox + user_version; надгробие: Unbanned позже Banned отменяет эффект | как выше | DLQ depth |

## 2. Каталог и инвентарь GPU (владелец – Inventory)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| GpuTypePublished | `inventory.gpu_type.published` | v1 / gpu_type_version=1 | outbox + relay | Rental (тарифы, квоты), Billing (прайсинг), Workloads (рантаймы) | внутри gpu_type_id: первое событие | inbox; повтор не пересоздаёт тип (UNIQUE gpu_type_id) | временные → retry → DLQ; постоянные → сразу DLQ | DLQ depth |
| GpuNodeRegistered | `inventory.node.registered` | v1 / node_version=1 | outbox + relay | Rental (пул узлов), мониторинг | да: Registered раньше Online/Offline того же node_id | inbox; повтор не дублирует узел (UNIQUE node_id) | как выше | узлы без heartbeat после Registered |
| GpuNodeOnline | `inventory.node.online` | v1 / node_version++ | outbox + relay | Rental (узел доступен для hold), алертинг | да: после Registered | inbox + node_version; повтор перехода online – пропуск по состоянию | как выше | узел online, но heartbeat пропал |
| GpuNodeOffline | `inventory.node.offline` | v1 / node_version++ | outbox + relay | Rental (не выдавать новые hold), Workloads (предупреждение/эвакуация jobs) | да: после Registered; надгробие: Offline позже Online | inbox + node_version | как выше | число jobs на offline-узле |
| GpuNodeDecommissioned | `inventory.node.decommissioned` | v1 / node_version++ | outbox + relay | Rental (исключить из пула), Workloads | да: после Registered | inbox + node_version; повтор пропуск по состоянию | как выше | аренды на списанном узле > 0 |
| GpuInventoryUpdated | `inventory.inventory.updated` | v1 / – (факт, высокая частота) | outbox + relay | Rental.waitlist (предложения очереди), аналитика | нет | inbox; повтор не пересоздаёт предложение (idempotency по transfer_id) | временные → retry → DLQ; частый поток – батчинг при чтении | лаг предложений очереди: free_count > 0, очередь не пуста |

## 3. Аренда и бронирование (владелец – Rental)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| RentalRequested | `rental.rental.requested` | v1 / rental_version=1 | outbox + relay | Billing (проверка баланса/платёж) | первое событие агрегата | inbox; повтор не создаёт вторую заявку (UNIQUE rental_id) | временные → retry → DLQ; постоянные → сразу DLQ | DLQ depth, возраст outbox |
| GpuHeld | `rental.hold.granted` | v1 / rental_version++ | outbox + relay | Billing (к оплате), уведомления | да: после Requested того же rental_id | inbox; повтор не переводит узел в hold дважды (состояние узла FREE→HELD под блокировкой) | как выше | HELD-узлы с истёкшим TTL в Redis |
| RentalConfirmed | `rental.rental.confirmed` | v1 / rental_version++ | outbox + relay | Workloads (готовить workspace), Inventory (узел занят), Billing (старт списания) | да: после Held; опоздавшее подтверждение hold'а игнорируется состоянием/сроком | inbox; **инвариант: одна GPU – одна аренда**, enforced состоянием узла под блокировкой, а не событием | как выше | CONFIRMED без WorkspaceProvisioned (lag Workloads) |
| RentalStarted | `rental.rental.started` | v1 / rental_version++ | outbox + relay | Workloads (provision workspace), Billing (старт metering) | да: после Confirmed | inbox + версия; повтор пропуск по состоянию | как выше | Started без workspace > 2 мин |
| RentalExtended | `rental.rental.extended` | v1 / rental_version++ | outbox + relay | Billing (пересчёт лимита), Workloads (deadline) | да: внутри rental_id | inbox; повтор не продлевает дважды (новый ends_at применяется по версии) | как выше | DLQ depth |
| RentalExpired | `rental.rental.expired` | v1 / rental_version++ | outbox + relay | Inventory (освобождение hold), Rental.waitlist (предложить узел), Billing (отмена неоплаченного) | да: после Held того же rental_id | inbox; повтор не перевыпускает предложение очереди (idempotency по transfer_id) | как выше | EXPIRED-аренды с занятым узлом |
| RentalCancelled | `rental.rental.cancelled` | v1 / rental_version++ | outbox + relay | Inventory (освобождение), Billing (возврат удержанного), Workloads (если была started) | да: внутри rental_id; надгробие отменён | inbox + версия | как выше | DLQ depth |
| RentalFinished | `rental.rental.finished` | v1 / rental_version++ (терминальное) | outbox + relay | Inventory (узел в пул → inventory.updated), Workloads (release workspace), Billing (финальный метринг), Rental.waitlist | да: после Started/Confirmed; терминальное – надгробие | inbox; повтор освобождения – пропуск по состоянию узла | как выше | Finished без WorkspaceReleased (lag Workloads) |
| WaitlistJoined | `rental.waitlist.joined` | v1 / waitlist_version=1 | outbox + relay | уведомления, аналитика | первое событие заявки | inbox; активная заявка пользователя на тип GPU уникальна (частичный UNIQUE) | временные → retry → DLQ | глубина очередей по типам GPU |
| WaitlistOfferCreated | `rental.offer.created` | v1 / offer_version=1 | outbox + relay | Billing (инvoice по предложению), уведомления | да: после Joined того же waitlist_id; FIFO очереди защищён блокировкой, не брокером | inbox; UNIQUE(offer_id); повтор не создаёт второе удержание | как выше | предложения без ответа, respond_until прошёл |
| WaitlistOfferAccepted | `rental.offer.accepted` | v1 / offer_version++ | outbox + relay | Rental (команда: создать hold/аренду по offer_id) | да: после Created, до Expired того же offer_id | inbox; **UNIQUE(source_offer_id)** – вторая бронь по тому же предложению невозможна | отказ «предложение истекло/продажи закрыты» – постоянный → сразу DLQ | DLQ depth, accepted без аренды |
| WaitlistOfferExpired | `rental.offer.expired` | v1 / offer_version++ (терминальное) | outbox + relay | Rental.waitlist (предложить следующему) | да: после Created | inbox; повтор не снимает удержание дважды (проверка актуальности передачи) | как выше | глубина очереди растёт при наличии free GPU |

## 4. Оплата и биллинг (владелец – Billing)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| BalanceToppedUp | `billing.balance.topped_up` | v1 / ledger entry (append-only) | outbox + relay | Rental (проверить ожидающие оплаты аренды), уведомления | нет (ledger append-only) | inbox; ledger-записи append-only, повтор не меняет баланс дважды (entry_id уникален) | временные → retry → DLQ | баланс противоречит сумме ledger-записей |
| PaymentStarted | `billing.payment.started` | v1 / payment_version=1 | outbox + relay | Rental (hold ждёт оплаты), уведомления | первое событие платежа | inbox; UNIQUE(payment_id); провайдер вызван с ключом `provider_payment_id` | временные (провайдер 5xx/таймаут) → retry с backoff+jitter, предел → DLQ; постоянные (4xx) → сразу DLQ. Повтор после таймаута – только с тем же ключом | возраст STARTED-платежей, DLQ depth |
| PaymentReceived | `billing.payment.received` | v1 / payment_version++ | outbox + relay | Rental (подтвердить аренду), Billing (ledger) | да: после Started того же payment_id; опоздавшее получение – решение Rental по состоянию/сроку | inbox; повтор не подтверждает аренду дважды (переход только из ожидающих состояний) | как выше | Received без RentalConfirmed (lag Rental) |
| PaymentFailed | `billing.payment.failed` | v1 / payment_version++ | outbox + relay | Rental (снять hold по таймауту/предложить повтор), уведомления | да: после Started | inbox; повтор не снимает hold дважды (TTL в Redis сам гасит) | постоянные по природе – публикуются всегда, DLQ не нужен | всплеск failed по одному payment_id |
| UsageMetered | `billing.usage.metered` | v1 / – (факт, высокая частота) | outbox + relay (батчами) | Billing (списание с баланса), аналитика (копия в ClickHouse), Rental (баланс тает) | нет; допускается пропуск интервалов, сверка по сумме | inbox; агрегат по (rental_id, interval) – повтор интервала не списывает дважды (UNIQUE) | временные → retry; **отдельная DLQ с длинным retention + сверка с ClickHouse** | lag метринга: списаний меньше, чем usage-записей; отставание копии в ClickHouse |
| InvoiceIssued | `billing.invoice.issued` | v1 / invoice_version=1 | outbox + relay | уведомления, Billing (хранение) | нет | inbox; UNIQUE(invoice_id) | временные → retry → DLQ | DLQ depth |
| RefundIssued | `billing.refund.refunded` | v1 / refund_version=1 | outbox + relay | Rental (освобождение/отмена по причине), уведомления | да: после PaymentReceived того же payment_id | inbox; UNIQUE(refund_id) – второй возврат невозможен; провайдер – с ключом возврата | ошибка возврата у провайдера: деньги зависли – алерт, не DLQ-молчание | refund в process > 1 ч, DLQ depth |
| BalanceDepleted | `billing.balance.depleted` | v1 / – (факт) | outbox + relay | Rental (остановить аренду), Workloads (cancel jobs), уведомления | нет; шторм повторов при непрерывном списании – дедуп по (user_id, rental_id, окно) | inbox; повтор не роняет аренду дважды (состояние) | временные → retry → DLQ | аренды ACTIVE у пользователей с нулевым балансом |

## 5. Запуск LLM-нагрузок (владелец – Workloads)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| WorkspaceProvisioned | `workloads.workspace.provisioned` | v1 / workspace_version=1 | outbox + relay | пользователь (endpoint), Billing (тарификация опций) | после RentalConfirmed/Started (внешний предшественник) | inbox; UNIQUE(workspace_id), связь с rental_id уникальна | временные → retry → DLQ | Confirmed без Provisioned (lag) |
| ModelDeployRequested | `workloads.deploy.requested` | v1 / deploy_version=1 | outbox + relay | оркестратор Workloads (планировщик GPU) | первое событие деплоя | inbox; UNIQUE(deploy_id) | как выше | очередь деплоев, DLQ depth |
| ModelDeployed | `workloads.deploy.deployed` | v1 / deploy_version++ | outbox + relay | пользователь, Billing (тариф инференса), мониторинг | да: после Requested того же deploy_id | inbox + версия; повтор пропуск по состоянию | как выше | deployed без endpoint-healthcheck |
| ModelDeployFailed | `workloads.deploy.failed` | v1 / deploy_version++ (терминальное) | outbox + relay | пользователь, поддержка | да: после Requested | inbox; надгробие: повторный deploy – новый deploy_id | постоянные (образ не найден, OOM шаблона) – событие, не ошибка обработки | всплеск failed по одному model_ref |
| JobSubmitted | `workloads.job.submitted` | v1 / job_version=1 | outbox + relay | планировщик Workloads | первое событие job | inbox; UNIQUE(job_id); повтор не ставит job в очередь дважды | отказ «нет активной аренды/workspace» – постоянный → сразу DLQ | очередь jobs |
| JobStarted | `workloads.job.started` | v1 / job_version++ | outbox + relay | Billing (учёт времени), аналитика | да: после Submitted | inbox + версия | как выше | STARTED-jobs без heartbeat аренды |
| JobCompleted | `workloads.job.completed` | v1 / job_version++ (терминальное) | outbox + relay | пользователь (result_uri в S3), Billing (финальный метринг) | да: после Started | inbox; повтор не перезаписывает результат (result_uri иммутабелен) | как выше | COMPLETED без result_uri |
| JobFailed | `workloads.job.failed` | v1 / job_version++ (терминальное) | outbox + relay | пользователь, поддержка | да: после Started | inbox; надгробие | как выше | всплеск failed по узлу (признак деградации GPU) |
| JobCancelled | `workloads.job.cancelled` | v1 / job_version++ (терминальное) | outbox + relay | Billing (стоп метринга), планировщик | да: после Submitted/Started; надгробие | inbox; повтор не останавливает дважды | как выше | DLQ depth |
| WorkspaceReleased | `workloads.workspace.released` | v1 / workspace_version++ (терминальное) | outbox + relay | Billing (стоп тарификации), мониторинг дисков/S3 | да: после Provisioned | inbox; повтор освобождения – пропуск по состоянию | как выше | Finished-аренды с неосвобождёнными workspace |

## 6. Админка (владелец – Admin)

| Событие | Ключ | Схема/вер. сущности | Публикация | Подписчики | Порядок | Идемпотентность | Ошибки | Дежурному |
|---|---|---|---|---|---|---|---|---|
| SupportTicketCreated | `admin.ticket.created` | v1 / ticket_version=1 | outbox + relay | поддержка (очередь), уведомления | первое событие тикета | inbox; UNIQUE(ticket_id) | временные → retry → DLQ | SLA открытых тикетов |
| SupportTicketResolved | `admin.ticket.resolved` | v1 / ticket_version++ (терминальное) | outbox + relay | пользователь, отчётность | да: после Created | inbox; повтор пропуск по состоянию | как выше | DLQ depth |
| GpuQuotaChanged | `admin.quota.changed` | v1 / quota_version++ | outbox + relay | Rental (проверка лимитов при новых заявках), Billing | внутри (user_id, gpu_type_id) | inbox; применение по версии, старое событие отбрасывается | как выше | заявки сверх новой квоты |
| AbuseReportFiled | `admin.abuse.filed` | v1 / report_version=1 | outbox + relay | модерация (очередь), мониторинг | первое событие жалобы | inbox; UNIQUE(report_id) | как выше | очередь нерешённых жалоб |
| AbuseReportReviewed | `admin.abuse.reviewed` | v1 / report_version++ (терминальное) | outbox + relay | Users (ban/unban по решению), Rental, Billing | да: после Filed того же report_id | inbox; повтор не дублирует последствия решения | как выше | reviewed с решением ban без UserBanned (lag Users) |

## Порядок: сводка

Порядок нужен только внутри одного агрегата: `user_id`, `node_id`, `rental_id`
(Requested → Held → Confirmed → Started → Extended/Expired/Cancelled →
Finished), `payment_id` (Started → Received/Failed/Refund), `offer_id`
(Created → Accepted/Expired), `deploy_id` (Requested → Deployed/Failed),
`job_id` (Submitted → Started → Completed/Failed/Cancelled), `workspace_id`
(Provisioned → Released). Обеспечивается: outbox + relay сохраняют порядок
записи; `aggregate_version` и надгробия состояний у получателя (опоздавшее
событие отбрасывается по состоянию, а не роняет обработчик); узел нельзя
подтвердить двум арендам – инвариант живёт в одном кластере (Inventory/
Rental) под блокировкой строки, а не в брокере. Глобальной упорядоченности
между сервисами не требуется; Rental ↔ Billing согласованы Saga с
компенсациями.

## DLQ

Очередь: `«очередь потребителя».dlq` (durable) + таблица `dead_letters` как
архив для поиска и группировки. Владелец DLQ – команда потребителя очереди,
даже если виноват отправитель.

**Что кладём:** исходные байты без изменений, тот же `event_id`;
заголовки `x-error-class`, `x-error`, `x-attempts`, `x-first-attempt-at`,
`x-dead-at`, `x-original-routing-key`, `x-original-exchange`, `x-trace-id`;
время – целые миллисекунды.

**Когда:** постоянная ошибка (4xx, невалидная схема, нарушение бизнес-
правила) – сразу; временная не прошла за предел (3 попытки / 30 с суммарно,
для UsageMetered – свои лимиты с учётом RPS) – после предела; TTL/переполнение
очереди – страховочно брокером. Паузу держит брокер: wait-очереди
`*.wait.1s/2s/4s` (TTL + dead-letter-exchange обратно), backoff –
экспонента с full jitter, счётчик `x-attempt` в заголовках; повтор в обход
очереди запрещён (ломает порядок агрегата).

**Особые потоки:** `UsageMetered` – высокочастотный: отдельная DLQ с лимитом
длины и длинным retention, обязательная сверка с ClickHouse-копией
(пропуск интервала = расхождение сумм); `PaymentReceived/Failed` – сверка
с платёжным провайдером по `provider_payment_id` (идемпотентность провайдера
+ наша); `inventory.updated` и события waitlist – группировка при разборе
по `gpu_type_id`.

**Процесс:** алерт по появлению сообщений и по росту → тикет дежурному;
разбор с группировкой по `x-error-class` (тысячи сообщений → 3–4 причины);
решение: переотправить / исправить и переотправить / отбросить с пометкой.
Переотправка – избирательная, с ограничением скорости (≤4/с), в
идемпотентного получателя с исходным `event_id`. В норме DLQ пуста;
постоянный фон прячет новые проблемы.

**Хранение:** минимум 30 дней (срок отметок inbox/processed > срока DLQ),
лимит длины очереди, доступ по роли, аудит удалений.

**Куда смотрит дежурный (сводно):**
1. Возраст старейшей неотправленной outbox-строки любого сервиса > 1 мин →
   relay/брокер; нет лидера relay дольше нескольких секунд.
2. Глубина и рост каждой DLQ; появились строки `status='DEAD'` в inbox.
3. `processed`/`inbox` в статусе обработки с истёкшей арендой строки –
   захватили, не доделали, ack не отправлен.
4. Сквозные инварианты по проекту: CONFIRMED-аренды без workspace,
   Finished-аренды без WorkspaceReleased, ACTIVE-аренды у нулевого баланса,
   HELD-узлы с истёкшим TTL в Redis, free_count > 0 при непустой очереди,
   lag копии UsageMetered в ClickHouse.
