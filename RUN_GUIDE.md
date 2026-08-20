# Покроковий Гайд із Запуску та Налаштування: EconomyService

Цей документ містить повну інструкцію із розгортання, збірки, конфігурації та тестування мікросервісу фінансів **EconomyService** та плагіну **CoreSystem**.

---

## 📐 Загальна Архітектура

```mermaid
flowchart LR
    Postgres[(PostgreSQL :5432)] <--> EconomySvc[EconomyService Go :8084]
    Redis[(Redis :6379)] <--> EconomySvc
    LevelingSvc[LevelingService Go] -- "Pub/Sub leveling:events:levelup" --> Redis
    EconomySvc -- "Pub/Sub economy:notifications" --> CoreSystem[CoreSystem Paper Plugin]
    CoreSystem -- "Read player:economy:<uuid>" --> Redis
    CoreSystem -- "Claim Daily Bonus REST API :8084" --> EconomySvc
```

---

## ⚙️ Крок 1: Конфігурація сервісу (`config.json`)

Файл конфігурації знаходиться за шляхом `EconomyService/config.json`:

```json
{
  "database_url": "postgres://postgres:password@localhost:5432/minigames",
  "redis_addr": "localhost:6379",
  "redis_password": "",
  "metrics_port": ":8083",
  "api_port": ":8084"
}
```

### Налаштування через змінні оточення (Environment Variables):
Якщо ви розгортаєте сервіс у Docker/Production, ви можете замінити конфігурацію параметрами:
* `DATABASE_URL` — рядок підключення до PostgreSQL.
* `REDIS_ADDR` — адреса Redis сервера (`localhost:6379`).
* `REDIS_PASSWORD` — пароль до Redis.
* `API_PORT` — порт REST API (`:8084`).
* `METRICS_PORT` — порт метрик Prometheus (`:8083`).

---

## 🚀 Крок 2: Запуск Go-Мікросервісу (EconomyService)

1. Відкрийте термінал у папці сервісу:
   ```powershell
   cd "C:\Users\xlanyleeet\Documents\antigravity prjct\EconomyService"
   ```

2. Запустіть сервіс:
   ```powershell
   .\economy-service.exe
   # або через Go CLI:
   go run .
   ```

### Очікуваний лог у консолі:
```text
===========================================
 Starting Minigames EconomyService (Go)  
===========================================
Loaded configuration from config.json
Successfully connected to PostgreSQL
EconomyService Worker ID initialized: computer-54321
Successfully connected to Redis
Starting Economy REST API server on :8084
Starting Prometheus metrics on :8083
Listening for match results on Redis Stream: minigames:events:match_results
Listening for LevelUp rewards on Pub/Sub: leveling:events:levelup
EconomyService is fully operational and waiting for match results & level up events...
```

---

## ☕ Крок 3: Компіляція та Установка Плагіну (`CoreSystem`)

1. Відкрийте термінал у папці `CoreSystem`:
   ```powershell
   cd "C:\Users\xlanyleeet\Documents\antigravity prjct\CoreSystem"
   ```

2. Скомпілюйте плагін за допомогою Gradle:
   ```powershell
   .\gradlew.bat shadowJar
   ```

3. Скопіюйте `.jar` файл з `build/libs/CoreSystem-1.0-SNAPSHOT-all.jar` у папку `plugins/` вашого Minecraft Paper сервера.

---

## 🎮 Крок 4: Перевірка Команд та Механік у Грі

1. **Команда `/daily` (або `/dailybonus`):**
   - Відкриває 27-слотове GUI **«🎁 Щоденний Бонус за Вхід»**.
   - Натисніть на доступний золотий блок/скриню ➔ плагін виконає запит, зарахує монети, відтворить звук `ENTITY_EXPERIENCE_ORB_PICKUP` та заспавнить частинки тотему!
2. **Команда `/balance` (або `/coins`):**
   - Виводить у чат актуальний баланс: `💰 Ваші Монети: 14,500` `🎟️ Сезонні Жетони: 150`.
3. **Команда `/stats`:**
   - Голова гравця показує оновлений рядок `Монети` та `Жетони`.

---

## 🌐 Крок 5: Перевірка REST API та Тестування

Ви можете тестувати баланси та транзакції прямо з консолі або curl:

### 1. Перевірка балансу гравця:
```powershell
curl -X GET "http://localhost:8084/api/v1/economy/balance?uuid=ТВІЙ_MINECRAFT_UUID"
```

### 2. Зарахування або списання коштів:
```powershell
curl -X POST "http://localhost:8084/api/v1/economy/transaction" `
  -H "Content-Type: application/json" `
  -d "{\"uuid\": \"ТВІЙ_MINECRAFT_UUID\", \"currency\": \"coins\", \"amount\": 1000, \"source\": \"ADMIN_REWARD\", \"idempotency_key\": \"reward-123\"}"
```

### 3. Лідерборд найбагатших гравців:
```powershell
curl -X GET "http://localhost:8084/api/v1/economy/leaderboard"
```
