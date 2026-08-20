# EconomyService — Мікросервіс Економіки та Фінансів (Go)

`EconomyService` — це високоефективний Go-мікросервіс для обробки балансів гравців (Монети 🪙 та Сезонні Жетони 🎟️), виконання атомарних транзакцій з захистом від дуплікації (`idempotency_key`), обробки щоденних бонусів (Daily Login Streak 1-7 днів) та автоматичного зарахування монет з подій матчів та підняття рівнів.

---

## ⚡ Можливості (Features)

* **Подвійна Валюта:** 🪙 Монети (`coins`) та 🎟️ Сезонні Жетони (`seasonal_tokens`).
* **Event-Driven Автоматизація:** Слухач Redis Streams `minigames:events:match_results` та Pub/Sub `leveling:events:levelup` — нагороди за ігри та рівні зараховуються миттєво без HTTP-запитів.
* **Атомарність та Ідемпотентність:** PostgreSQL `BEGIN ... SELECT FOR UPDATE` з таблицею аудиту `economy_transactions` та перевіркою `idempotency_key`.
* **Daily Login Streak Engine:** Система щоденного бонусу за вхід (1-7 днів) з наростаючими нагородами та тайм-аутом 48 годин для скидання стріку.
* **Дворівневий кеш та лідерборд:** Redis Hash `player:economy:<uuid>` + Sorted Set `leaderboard:economy:coins`.
* **Live Notifications:** Публікація в Pub/Sub `economy:notifications` для миттєвих повідомлень у чаті Minecraft та звуку при отриманні монет.
* **REST HTTP API & Metrics:** Порт `:8084` для REST запитів та порт `:8083` для Prometheus метрик.

---

## 🚀 Інструкція із Запуску

### 1. Збірка за залежності
```powershell
go mod tidy
go test -v ./...
go build -o economy-service.exe .
```

### 2. Запуск
```powershell
.\economy-service.exe
```
