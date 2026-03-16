"""
Webhook-сервер для обработки запросов от триггеров Яндекс Трекера.

Эндпоинты:
  GET  /health                — проверка работоспособности
  POST /webhook/tags-sync     — синхронизация тегов
  POST /webhook/priority-sync — синхронизация businessPriority
  POST /webhook/full-sync     — синхронизация тегов + businessPriority

Настройка триггера в UI Трекера:
  Условие: «Значение поля изменилось» + фильтр по типу задачи (Эпик)
  Метод:   POST
  Тело:    {"issue_key": "{{issue.key}}"}

Защита от каскада: tracker_client проверяет наличие parent у задачи
и пропускает подзадачи (статус SKIPPED). Это предотвращает бесконечные
вызовы при обновлении полей подзадач через PATCH.
"""
import time

from flask import Flask, request, jsonify, abort

import config
from logger import get_logger
from tracker_client import (
    sync_tags_to_subtasks,
    sync_business_priority_to_subtasks,
    sync_all_fields_to_subtasks,
    sync_tags_to_blockers,
    sync_business_priority_to_blockers,
    sync_all_fields_to_blockers,
    TrackerAPIError,
)

logger = get_logger(__name__)
app    = Flask(__name__)


def _check_secret() -> bool:
    """Проверить заголовок X-Tracker-Secret."""
    if not config.WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Tracker-Secret") == config.WEBHOOK_SECRET


def _get_issue_key() -> str:
    """
    Извлечь issue_key из JSON-тела запроса.
    abort(403) — неверный секрет.
    abort(400) — пустое тело или отсутствует issue_key.
    """
    if not _check_secret():
        logger.warning("Неверный секрет от %s", request.remote_addr)
        abort(403, description="Invalid secret")

    data = request.get_json(silent=True)
    if not data:
        abort(400, description="Пустое тело или не JSON")

    issue_key = data.get("issue_key")
    if not issue_key:
        abort(400, description="Поле 'issue_key' отсутствует")

    return issue_key


def _handle(sync_fn, issue_key: str) -> tuple:
    """
    Вызвать sync-функцию и вернуть HTTP-ответ.

    Коды ответов:
      200 — успех или SKIPPED (включая PARTIAL)
      400 — некорректный запрос   (Трекер не повторяет)
      403 — неверный секрет       (Трекер не повторяет)
      502 — ошибка Трекер API     (Трекер не повторяет)
      500 — неожиданная ошибка    (Трекер повторит до 5 раз)
    """
    t0 = time.monotonic()
    logger.info("→ %s [%s]", sync_fn.__name__, issue_key)

    try:
        result  = sync_fn(issue_key)
        elapsed = round(time.monotonic() - t0, 3)

        logger.info(
            "← %s status=%s updated=%d/%d errors=%d elapsed=%.3fs",
            issue_key,
            result.get("status"),
            len(result.get("updated",  [])),
            len(result.get("subtasks", [])),
            len(result.get("errors",   [])),
            elapsed,
        )

        for e in result.get("errors", []):
            logger.warning("  ✗ %s: %s", e.get("issue"), e.get("error"))

        return jsonify(result), 200

    except TrackerAPIError as exc:
        logger.error("API ошибка [%s]: %s", issue_key, exc)
        return jsonify({"error": str(exc)}), 502

    except Exception:
        logger.exception("Неожиданная ошибка [%s]", issue_key)
        return jsonify({"error": "Internal server error"}), 500


# ──────────────────────────────────────────────────────────────
# Маршруты
# ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/webhook/tags-sync")
def webhook_tags_sync():
    """Триггер: Теги → «Значение поля изменилось»"""
    return _handle(sync_tags_to_subtasks, _get_issue_key())


@app.post("/webhook/priority-sync")
def webhook_priority_sync():
    """Триггер: businessPriority → «Значение поля изменилось»"""
    return _handle(sync_business_priority_to_subtasks, _get_issue_key())


@app.post("/webhook/full-sync")
def webhook_full_sync():
    """Триггер: Теги ИЛИ businessPriority → «Значение поля изменилось»"""
    return _handle(sync_all_fields_to_subtasks, _get_issue_key())

@app.post("/webhook/cross-queue-tags-sync")
def webhook_cross_queue_tags():
    """
    Синхронизировать ТЕГИ из ENG-эпика → блокирующие BACKEND-эпики.

    Триггер ENGINEERING: Тип задачи = Эпик, Теги → «Значение поля изменилось»
    Тело: {"issue_key": "{{issue.key}}"}
    """
    return _handle(sync_tags_to_blockers, _get_issue_key())


@app.post("/webhook/cross-queue-priority-sync")
def webhook_cross_queue_priority():
    """
    Синхронизировать BUSINESS PRIORITY из ENG-эпика → блокирующие BACKEND-эпики.

    Триггер ENGINEERING: Тип задачи = Эпик, businessPriority → «Значение поля изменилось»
    Тело: {"issue_key": "{{issue.key}}"}
    """
    return _handle(sync_business_priority_to_blockers, _get_issue_key())



# ──────────────────────────────────────────────────────────────
# Обработчики ошибок Flask
# ──────────────────────────────────────────────────────────────

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": str(e.description)}), 400

@app.errorhandler(403)
def forbidden(e):
    return jsonify({"error": str(e.description)}), 403


if __name__ == "__main__":
    logger.info("Запуск на %s:%s", config.WEBHOOK_HOST, config.WEBHOOK_PORT)
    app.run(host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT, debug=False)
