# Запуск проєкту та приклади запитів

Цей документ містить лише необхідне для налаштування, запуску й перевірки проєкту через Postman та MCP Inspector. Усі команди Docker виконуй у PowerShell з кореневої папки проєкту.

## 1. Налаштування

Потрібні Docker Desktop із запущеним Docker Engine та Postman. Для Function Calling потрібен також API-ключ OpenAI. Для звичайного REST API, MCP і A2A OpenAI-ключ не потрібний.

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
```

Якщо `.env` вже існує, не копіюй шаблон повторно — просто відредагуй файл.

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

Потрібен OpenAI API-ключ у `.env` і запущений `producer-agent`. Створи запит `AI chat`:

- Method: `POST`
- URL: `{{base_url}}/ai/chat`
- Authorization: Bearer Token `{{access_token}}`
- Body:

```json
{
  "prompt": "Мій акаунт має email ai-test@example.com. Покажи мої задачі.",
  "confirm_writes": false
}
```

Для перевірки створення/оновлення задачі попроси про конкретну зміну й встанови `"confirm_writes": true`. Це дозволяє моделі змінювати тестові дані. Запит без цього прапорця може читати дані, але запис буде відхилено.

## 7. MCP через MCP Inspector

MCP — це окремий протокол, тому цей тест виконуй у MCP Inspector, а не звичайним REST-запитом у Postman:

1. Запусти проєкт командою `docker compose up --build -d`.
2. У MCP Inspector вибери транспорт **Streamable HTTP**.
3. Вкажи URL `http://localhost:8004/mcp`.
4. Додай HTTP header `Authorization` зі значенням `Bearer <access_token>`.
5. Під'єднайся та перевір tools `get_current_account`, `list_tasks`, `list_task_lists`, а також resources `todo://me/account` і `todo://me/tasks`.
6. Для створення задачі виклич tool `create_task` з аргументами `title` і `confirmation: "CONFIRM"`. Для оновлення також додай `confirmation: "CONFIRM"`. Видалення вимагає `confirmation: "DELETE"`.

Підтвердження у вигляді аргументу має передаватися MCP-клієнтом після погодження користувача; сам рядок не є криптографічним підтвердженням.

## 8. A2A Producer → Consumer через Postman

A2A-запит до Producer створить задачу, а Producer передасть подію Consumer-у, який створить сповіщення. Створи запит `A2A create task and notify`:

- Method: `POST`
- URL: `{{base_url}}/a2a/jsonrpc`
- Authorization: Bearer Token `{{access_token}}`
- Headers:
  - `Content-Type: application/json`
  - `A2A-Version: 1.0`
- Body:

```json
{
  "jsonrpc": "2.0",
  "id": "postman-a2a-1",
  "method": "message/send",
  "params": {
    "message": {
      "messageId": "postman-message-1",
      "contextId": "postman-context-1",
      "role": "user",
      "parts": [
        {
          "kind": "text",
          "text": "{\"title\":\"A2A тестова задача\",\"description\":\"Тест Producer і Consumer\",\"due_date\":null}"
        }
      ]
    }
  }
}
```

Успішна відповідь означає, що Producer завершив обробку та отримав підтвердження від Consumer. Перевір результат запитами з Bearer-токеном:

- `GET {{base_url}}/tasks` — має з'явитися `A2A тестова задача`.
- `GET {{base_url}}/notifications` — має з'явитися сповіщення про неї.

Agent Cards можна відкрити в браузері або Postman:

- Producer: `GET http://localhost:8010/.well-known/agent-card.json`
- Consumer: `GET http://localhost:8011/.well-known/agent-card.json`

Для діагностики A2A:

```powershell
docker compose logs --tail=100 producer-agent consumer-agent
```

> A2A передає той самий Bearer-токен у внутрішньому виклику Consumer-у. Токен усе ще навчальний і не має підпису. Не виставляй стенд у публічну мережу.
