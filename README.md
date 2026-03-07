# Tracker Sync App

Автоматическая синхронизация тегов Яндекс Трекер.
## 📁 Структура проекта

```
tracker_sync/
├── README.md              # Документация
├── .gitignore            # Настройки Git
└── app/                  # Исходный код
    ├── config.py         # Конфигурация (.env)
    ├── webhook_server.py # Flask webhook-сервер
    ├── tracker_client.py # Клиент Яндекс Трекер API v3
    ├── sync_all.py       # Массовое обновление
    ├── requirements.txt  # Зависимости
    └── logs/             # Логи (gitignore)
        └── app.log
```

## 🚀 Быстрый старт
