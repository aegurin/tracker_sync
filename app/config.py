"""Конфигурация из переменных окружения (.env)."""
import os
from dotenv import load_dotenv

load_dotenv()

TRACKER_TOKEN    = os.getenv("TRACKER_TOKEN", "")
TRACKER_ORG_ID   = os.getenv("TRACKER_ORG_ID", "")
TRACKER_ORG_TYPE = os.getenv("TRACKER_ORG_TYPE", "yandex360")
TRACKER_API_BASE = os.getenv("TRACKER_API_BASE", "https://api.tracker.yandex.net/v3")

WEBHOOK_HOST   = os.getenv("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT   = int(os.getenv("WEBHOOK_PORT", "8080"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

TRACKER_QUEUE_KEY = os.getenv("TRACKER_QUEUE_KEY", "")

# ──────────────────────────────────────────────────────────────
# Фильтрация очередей для кросс-очередной синхронизации (blockers)
# Список префиксов через запятую: "BACKENDTEAM,INFRA,PLATFORM"
# Пустая строка = разрешены ВСЕ очереди (без фильтра)
# ──────────────────────────────────────────────────────────────
BLOCKER_ALLOWED_QUEUES_RAW = os.getenv("BLOCKER_ALLOWED_QUEUES", "BACKENDTEAM")

BLOCKER_ALLOWED_QUEUES: list[str] = [
    q.strip().upper()
    for q in BLOCKER_ALLOWED_QUEUES_RAW.split(",")
    if q.strip()
]

# ──────────────────────────────────────────────────────────────
# Отладочные флаги
# ──────────────────────────────────────────────────────────────

# DRY_RUN=true — читает данные, логирует, но НЕ делает PATCH-запросы
DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"

# LOG_LINKS_RAW=true — логирует полный JSON всех связей задачи
LOG_LINKS_RAW: bool = os.getenv("LOG_LINKS_RAW", "false").lower() == "true"



def get_headers() -> dict:
    """HTTP-заголовки для авторизации в Яндекс Трекер API."""
    org_header = "X-Org-ID" if TRACKER_ORG_TYPE == "yandex360" else "X-Cloud-Org-ID"
    return {
        "Authorization": f"OAuth {TRACKER_TOKEN}",
        org_header: TRACKER_ORG_ID,
        "Content-Type": "application/json",
    }
