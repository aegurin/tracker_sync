"""
Webhook-сервер для обработки HTTP-запросов от триггеров Яндекс Трекера.

Поддерживаемые эндпоинты:
  GET  /health                     — проверка работоспособности
  POST /webhook/tags-sync          — синхронизация тегов
  POST /webhook/priority-sync      — синхронизация businessPriority
  POST /webhook/full-sync          — синхронизация тегов + businessPriority

Настройка триггеров в UI Трекера:
  Условие: «Значение поля изменилось» для нужного поля
  Тело:    {"issue_key": "{{issue.key}}"}
"""
import time

from flask import Flask, request, jsonify, abort

import config
from logger import get_logger
from tracker_client import (
    sync_tags_to_subtasks,
    sync_business_priority_to_subtasks,
    sync_all_fields_to_subtasks,
    TrackerAPIError,
)

logger = get_logger(__name__)
app = Flask(__name__)


def _check_secret() -> bool:
    """Проверить X-Tracker-Secret если WEBHOOK_SECRET задан."""
    if not config.WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Tracker-Secret") == config.WEBHOOK_SECRET


def _get_issue_key() -> str:
    """
    Извлечь issue_key из тела запроса.
    Вызывает abort(400/403) при ошибке валидации.
    """
    if not _check_secret():
        logger.warning("Неверный секрет от %s", request.remote_addr)
        abort(403, description="Invalid secret")

    data = request.get_json(silent=True)
    if not data:
        logger.warning("Пустое тело запроса от %s", request.remote_addr)
        abort(400, description="Пустое тело или не JSON")

    issue_key = data.get("issue_key")
    if not issue_key:
        logger.warning("Отсутствует issue_key: %s", data)
        abort(400, description="Поле \'issue_key\' отсутствует")

    return issue_key


def _handle(sync_fn, issue_key: str) -> tuple:
    """
    Общий обработчик вызова sync-функции с логированием времени.

    HTTP коды ответов:
        200 — успех
        400 — плохой запрос     (Трекер НЕ повторяет)
        403 — неверный секрет   (Трекер НЕ повторяет)
        500 — внутренняя ошибка (Трекер повторит до 5 раз)
        502 — ошибка Трекер API (Трекер НЕ повторяет)
    """
    start = time.monotonic()
    logger.info("Webhook: %s → %s", sync_fn.__name__, issue_key)
    try:
        result = sync_fn(issue_key)
        elapsed = round(time.monotonic() - start, 3)
        logger.info(
            "Готово: issue=%s status=%s subtasks=%d elapsed=%.3fs",
            issue_key,
            result.get("status"),
            len(result.get("subtasks", [])),
            elapsed,
        )
        return jsonify(result), 200
    except TrackerAPIError as exc:
        logger.error("Ошибка Трекер API [%s]: %s", issue_key, exc)
        return jsonify({"error": str(exc)}), 502
    except Exception:
        logger.exception("Неожиданная ошибка [%s]", issue_key)
        return jsonify({"error": "Internal server error"}), 500


# ──────────────────────────────────────────────────────────────
# Эндпоинты
# ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Проверка работоспособности сервера."""
    return jsonify({"status": "ok"}), 200


@app.post("/webhook/tags-sync")
def webhook_tags_sync():
    """
    Синхронизировать теги родительской задачи → подзадачи.

    Триггер: Теги → «Значение поля изменилось»
    """
    return _handle(sync_tags_to_subtasks, _get_issue_key())


@app.post("/webhook/priority-sync")
def webhook_priority_sync():
    """
    Синхронизировать businessPriority родительской задачи → подзадачи.

    Триггер: businessPriority → «Значение поля изменилось»
    """
    return _handle(sync_business_priority_to_subtasks, _get_issue_key())


@app.post("/webhook/full-sync")
def webhook_full_sync():
    """
    Синхронизировать теги И businessPriority одним запросом → подзадачи.

    Триггер: Теги ИЛИ businessPriority → «Значение поля изменилось»
    """
    return _handle(sync_all_fields_to_subtasks, _get_issue_key())


# ──────────────────────────────────────────────────────────────
# Обработчики ошибок
# ──────────────────────────────────────────────────────────────

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": str(e.description)}), 400


@app.errorhandler(403)
def forbidden(e):
    return jsonify({"error": str(e.description)}), 403


# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Запуск на %s:%s", config.WEBHOOK_HOST, config.WEBHOOK_PORT)
    app.run(host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT, debug=False)
