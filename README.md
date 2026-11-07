# Запуск проєкту та приклади запитів

Цей документ містить лише необхідне для налаштування, запуску й перевірки проєкту через Postman та MCP Inspector. Усі команди Docker виконуй у PowerShell з кореневої папки проєкту.

## Event Storming сесія
!(C:\Users\niker\Downloads\eventstorming.png)

## Діаграма контекстів і компонентів
!(C:\Users\niker\Downloads\diagram.png)

## Архітектура мікросервісної системи
!(C:\Users\niker\Downloads\architecture.png)

## Мікросервісна діаграма викликів(Users)
!(C:\Users\niker\Downloads\users.png)

## Мікросервісна діаграма викликів(Tasks & Task Lists)
!(C:\Users\niker\Downloads\tasks.png)

## Мікросервісна діаграма викликів(Reminders & Notifications)
!(C:\Users\niker\Downloads\reminders.png)

## Структура проєкту

```text
SOAlab/
├── ai-integration/
│   ├── a2a_agent.py          # A2A Producer і Consumer
│   ├── function_calling.py   # Function Calling endpoint, tools і mock-режим
│   ├── mcp_server.py         # MCP tools, resources і prompt
│   ├── api_client.py         # Спільний клієнт для REST-сервісів
│   ├── ai_database.py        # Облік використання токенів
│   ├── requirements.txt      # Залежності AI-сервісів
│   └── Dockerfile
├── user-service/
│   ├── main.py               # Реєстрація, вхід і API користувачів
│   ├── schemas.py             # Схеми запитів і відповідей
│   ├── database.py            # Робота з базою даних користувачів
│   ├── rabbitmq.py             # Обмін подіями через RabbitMQ
│   ├── requirements.txt
│   └── Dockerfile
├── task-service/
│   ├── main.py               # API задач і списків, планування reminder
│   ├── schemas.py             # Схеми задач і списків
│   ├── database.py            # База даних і outbox
│   ├── rabbitmq.py             # Обробка та передавання подій
│   ├── requirements.txt
│   └── Dockerfile
├── notification-service/
│   ├── main.py               # API reminders і notifications та reminder worker
│   ├── schemas.py             # Схеми нагадувань і сповіщень
│   ├── database.py            # База даних і outbox
│   ├── rabbitmq.py             # Отримання подій від інших сервісів
│   ├── requirements.txt
│   └── Dockerfile
├── nginx/
│   └── nginx.conf             # Маршрутизація API-запитів до сервісів
├── docker-compose.yml         # Сервіси, мережа, порти й volumes
├── .env.example               # Шаблон змінних середовища
├── .dockerignore
├── .gitignore
└── PROJECT_EXPLANATION_UK.md  # Інструкція запуску й тестування
```

`docker-compose.yml` запускає сервіси та RabbitMQ у спільній Docker-мережі. Клієнтські запити до REST API та Function Calling проходять через Nginx. MCP-сервер і A2A-агенти запускаються окремими контейнерами, а всередині мережі звертаються до User, Task і Notification Service. Локальний `.env` містить власні налаштування та секрети, тому в структурі наведено лише безпечний шаблон `.env.example`.

## 1. Налаштування

Потрібні Docker Desktop із запущеним Docker Engine та Postman. Function Calling можна перевірити у mock-режимі без OpenAI API-ключа або у звичайному режимі з ключем. REST API, MCP і A2A OpenAI-ключа не потребують.

Якщо хочеш використовувати Function Calling, створи локальний `.env` із шаблону:

```powershell
Copy-Item .env.example .env
notepad .env
```

У файлі заміни значення `OPENAI_API_KEY` на власний ключ, збережи й закрий його. Не додавай ключ до Postman або `.env.example`. Файл `.env` ігнорується Git.

Приклад вмісту `.env`:

```dotenv
OPENAI_API_KEY=твій-ключ
OPENAI_MODEL=gpt-4o-mini
AI_FUNCTION_CALLING_MODE=openai
```

Якщо `.env` вже існує, не копіюй шаблон повторно — просто відредагуй файл.
Для роботи без API-кредитів установи `AI_FUNCTION_CALLING_MODE=mock`. Mock-режим не викликає OpenAI, не витрачає токени й підтримує лише описані нижче демонстраційні дії.

## 2. Запуск і зупинка

Зібрати образи та запустити всі сервіси у фоні:

```powershell
docker compose up --build -d
docker compose ps
```

Переглянути логи:

```powershell
docker compose logs --tail=100 user-service
docker compose logs --tail=100 task-service
docker compose logs --tail=100 notification-service
docker compose logs --tail=100 producer-agent
docker compose logs --tail=100 consumer-agent
docker compose logs --tail=100 mcp-server
```

Зупинити сервіси, зберігши дані:

```powershell
docker compose down
```

Не додавай `-v`, якщо потрібно зберегти користувачів і тестові дані у Docker volumes.

Основна адреса для запитів через Nginx — `http://localhost:8080`. Сервіси також доступні напряму: User Service на `8001`, Task Service на `8002`, Notification Service на `8003`. Інтерфейси Swagger:

- `http://localhost:8001/docs`
- `http://localhost:8002/docs`
- `http://localhost:8003/docs`

## 3. Налаштування Postman

Створи колекцію `SOAlab` і, за бажанням, environment із такими змінними:

| Змінна | Значення |
|---|---|
| `base_url` | `http://localhost:8080` |
| `access_token` | заповнити після входу |
| `user_id` | заповнити після реєстрації |
| `task_id` | заповнити після створення задачі |
| `list_id` | заповнити після створення списку |

Для запитів із JSON вибирай **Body → raw → JSON**. Для запитів, що потребують авторизації, встанови **Authorization → Bearer Token** і значення `{{access_token}}`.

## 4. Реєстрація та вхід

### Зареєструвати тестового користувача

Створи запит `Register user`:

- Method: `POST`
- URL: `{{base_url}}/users/register`
- Body:

```json
{
  "email": "ai-test@example.com",
  "password": "test-password"
}
```

З відповіді збережи `user.id` у змінну `user_id`. Реєструй цю адресу лише один раз. Якщо користувач уже є, переходь одразу до входу.

### Увійти

Створи запит `Login user`:

- Method: `POST`
- URL: `{{base_url}}/users/login`
- Body:

```json
{
  "email": "ai-test@example.com",
  "password": "test-password"
}
```

З відповіді збережи `access_token` у змінну `access_token`. Для захищених запитів передавай його як Bearer-токен.

> Токен у проєкті навчальний (`fake-jwt-token-...`), не має криптографічного підпису й не призначений для production.

## 5. REST API через Postman

### Створити список

`POST {{base_url}}/lists`, Bearer Token `{{access_token}}`:

```json
{
  "title": "Навчання"
}
```

Збережи `id` зі створеного списку у `list_id`.

### Створити задачу

`POST {{base_url}}/tasks`, Bearer Token `{{access_token}}`:

```json
{
  "title": "Перевірити API",
  "description": "Тестова задача",
  "list_id": "{{list_id}}",
  "due_date": null
}
```

Збережи `task.id` у `task_id`. Для дати використовуй ISO 8601, наприклад `2030-01-02T15:30:00`. Створення задачі з датою запускає Saga для створення нагадування; для першої перевірки можна залишити `due_date` рівним `null`.

### Переглянути й оновити задачі

- `GET {{base_url}}/tasks` — список задач.
- `GET {{base_url}}/tasks/{{task_id}}` — одна задача.
- `PUT {{base_url}}/tasks/{{task_id}}` — змінити задачу:

```json
{
  "title": "Оновлена назва"
}
```

Для оновлення метод `PUT` приймає поля `title`, `description`, `status`, `due_date`.

### Створити нагадування

`POST {{base_url}}/reminders`, Bearer Token `{{access_token}}`:

```json
{
  "task_id": "{{task_id}}",
  "reminder_time": "2030-01-02T15:00:00",
  "is_active": true
}
```

Переглянути нагадування: `GET {{base_url}}/reminders`.

### Переглянути сповіщення

`GET {{base_url}}/notifications`, Bearer Token `{{access_token}}`.

Вручну надіслати тестове сповіщення можна так:

`POST {{base_url}}/notifications/send`, Bearer Token `{{access_token}}`:

```json
{
  "user_id": "{{user_id}}",
  "message": "Тестове сповіщення",
  "channel": "push"
}
```

### Видалити тестову задачу

`DELETE {{base_url}}/tasks/{{task_id}}`, Bearer Token `{{access_token}}`.

## 6. Function Calling через Postman

### Перевірка без OpenAI API-ключа

У `.env` установи:

```dotenv
AI_FUNCTION_CALLING_MODE=mock
```

Для цього режиму `OPENAI_API_KEY` не потрібен. Пересоздай лише Producer Agent, щоб він прочитав нову конфігурацію:

```powershell
docker compose up -d --force-recreate producer-agent
```

Образ перебудовувати не потрібно, якщо код не змінювався.

Створи запит `AI chat`:

- Method: `POST`
- URL: `{{base_url}}/ai/chat`
- Authorization: Bearer Token `{{access_token}}`
- Body:

```json
{
  "prompt": "Email: ai-test@example.com\nList my tasks.",
  "confirm_writes": false
}
```

Mock-режим шукає email у prompt, перевіряє його через tool `verify_account_email`, а потім викликає `list_tasks`. Відповідь містить `"mode": "mock"`, `"model": "mock"`, `"tokens_used": 0` і список `"tools_called"`; у `reply` виводяться назви знайдених задач або повідомлення, що задач немає. Це перевіряє зв'язок endpoint → tools → REST API, але не перевіряє справжню LLM.

Щоб перевірити створення тестової задачі, надішли:

```json
{
  "prompt": "Email: ai-test@example.com\nCreate task: Mock function calling test",
  "confirm_writes": true
}
```

Задача створиться через Task Service. Перевір її через `GET {{base_url}}/tasks`. Без `"confirm_writes": true` створення буде відхилено.

Для повернення до справжнього OpenAI-виклику зміни `AI_FUNCTION_CALLING_MODE=openai` у `.env` і пересоздай Producer Agent тією ж командою. Для цього режиму потрібен чинний OpenAI API-ключ і доступний API-білінг.

## 7. MCP через MCP Inspector

MCP — це окремий протокол, тому цей тест виконуй у MCP Inspector, а не звичайним REST-запитом у Postman:

1. Запусти потрібні сервіси командою `docker compose up -d --build mcp-server notification-service`.
2. Запусти Inspector в окремому вікні PowerShell:

   ```powershell
   docker run --rm -p 127.0.0.1:6274:6274 ghcr.io/modelcontextprotocol/inspector
   ```

   Перший запуск завантажить Docker-образ Inspector. Залиш це вікно відкритим і відкрий адресу, яку команда надрукує, або `http://localhost:6274`.
3. У MCP Inspector натисни **Add Servers → Add manually**, вибери транспорт **Streamable HTTP** і вкажи URL `http://host.docker.internal:8004/mcp`. Оскільки Inspector запущений у Docker, тут потрібен `host.docker.internal`, а не `localhost`.
4. Додай HTTP header `Authorization` зі значенням `Bearer <access_token>`.
5. Під'єднайся та перевір tools `get_current_account`, `list_tasks`, `list_task_lists`, `list_reminders` і `list_notifications`, а також URI templates `todo://me/account/{user_id}` і `todo://me/tasks/{user_id}`. Для `{user_id}` використовуй `id` із `get_current_account`; сервер звіряє його з автентифікованим акаунтом.
6. Для створення задачі виклич tool `create_task` з аргументами `title` і `confirmation: "CONFIRM"`. Для оновлення також додай `confirmation: "CONFIRM"`. Видалення вимагає `confirmation: "DELETE"`.
7. `create_reminder` перевіряє через Task Service, що задача існує й належить поточному користувачу. MCP не надає окремого tool для негайного надсилання notification: notification створюється reminder worker-ом, коли настає час нагадування.

Підтвердження у вигляді аргументу має передаватися MCP-клієнтом після погодження користувача; сам рядок не є криптографічним підтвердженням.

## 8. A2A Producer → Consumer через Postman

Повний флоу: Postman надсилає A2A-запит Producer-у → Producer створює задачу через Task Service → Task Service планує reminder на `due_date` → Producer передає `task_id` Consumer-у → Consumer перевіряє задачу й підтверджує обробку → коли настає `due_date`, Notification Service створює notification. До настання терміну notification не створюється.

Перед тестом переконайся, що запущені `producer-agent`, `consumer-agent`, `task-service`, `notification-service` і `rabbitmq` (`docker compose ps`), а в Postman після `Login user` встановлена змінна `{{access_token}}`. Додай у Postman environment змінну `due_date` з майбутнім часом ISO 8601, наприклад `2030-01-02T15:00:00+02:00`. Щоб перевірити появу notification під час тесту, встанови її приблизно на 1–2 хвилини вперед від поточного локального часу. Перед запуском переглянь `GET {{base_url}}/tasks`, `GET {{base_url}}/reminders` і `GET {{base_url}}/notifications`, щоб відрізняти нові записи.

Створи запит `A2A create task and schedule reminder`:

- Method: `POST`
- URL: `{{base_url}}/a2a/jsonrpc`
- Authorization: Bearer Token `{{access_token}}`
- Headers:
  - `Content-Type: application/json`
  - `A2A-Version: 1.0`
- У вкладці Authorization обери тип **Bearer Token** і введи `{{access_token}}`.
- Body:

```json
{
  "jsonrpc": "2.0",
  "id": "{{$guid}}",
  "method": "SendMessage",
  "params": {
    "message": {
      "messageId": "{{$guid}}",
      "contextId": "{{$guid}}",
      "role": "ROLE_USER",
      "parts": [
        {
          "text": "{\"title\":\"A2A reminder flow test\",\"description\":\"Перевірка Producer, Consumer і reminder\",\"due_date\":\"{{due_date}}\"}"
        }
      ]
    }
  }
}
```

Переконайся, що заголовок `A2A-Version` увімкнений і має саме значення `1.0`. Метод `SendMessage` і роль `ROLE_USER` потрібні для A2A 1.0.

Успішна відповідь має містити `result.task.status.state: "TASK_STATE_COMPLETED"`. У `result.task.artifacts` відкрий текст результату: там буде створена задача та повідомлення, що reminder запланований, а notification буде створено, коли настане час. Це підтвердження обробки події Consumer-ом, а не відправка notification. Перевір результат запитами з тим самим Bearer-токеном:

- `GET {{base_url}}/tasks` — має з'явитися `A2A reminder flow test` із заданим `due_date`.
- `GET {{base_url}}/reminders` — має з'явитися активний reminder зі статусом `PENDING` і часом `due_date`.
- До настання `due_date` у `GET {{base_url}}/notifications` не має з'явитися новий запис для створеної задачі.

Дочекайся `due_date` плюс до 10 секунд: Notification Service перевіряє reminders кожні 10 секунд. Після цього `GET {{base_url}}/reminders` має показати статус `TRIGGERED`, а `GET {{base_url}}/notifications` — notification із `task_id`, що збігається з ID створеної задачі. Проєкт створює notification-запис у Notification Service; це не означає реальне надсилання push-повідомлення на пристрій.

Якщо передати `"due_date": null`, reminder і notification не створюються. A2A-відповідь `TASK_STATE_COMPLETED` підтверджує обробку події Consumer-ом, а не негайну відправку notification.

Якщо A2A-відповідь має стан `TASK_STATE_FAILED` або немає reminder, перевір логи:

```powershell
docker compose logs --tail=100 producer-agent consumer-agent
docker compose logs --tail=100 task-service notification-service
```

Agent Cards можна відкрити в Postman або браузері:

- Producer: `GET http://localhost:8010/.well-known/agent-card.json`
- Consumer: `GET http://localhost:8011/.well-known/agent-card.json`

> A2A передає той самий Bearer-токен у внутрішньому виклику Consumer-у. Токен усе ще навчальний і не має підпису. Не виставляй стенд у публічну мережу.
