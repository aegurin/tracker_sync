"""
Webhook-сервер для обработки HTTP-запросов от триггеров Яндекс Трекера.

Синхронизирует теги задачи ЛЮБОГО типа со всеми её прямыми подзадачами
через Bulk Change API одним запросом.

Триггер настраивается вручную в UI Трекера:
  Настройки очереди → Автоматизация → Триггеры → Создать триггер
  Условие:  Теги → «Значение поля изменилось»
  Действие: HTTP POST → {WEBHOOK_URL}/webhook/tags-sync
  Тело:     {"issue_key": "{{issue.key}}"}

Яндекс Трекер ожидает ответ за 10 сек.
При HTTP 500 или таймауте повторяет запрос до 5 раз.
"""
import logging
import sys

from flask import Flask, request, jsonify, abort

import config
from tracker_client import sync_tags_to_subtasks, TrackerAPIError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def _check_secret() -> bool:
    """
    Проверить заголовок X-Tracker-Secret, если WEBHOOK_SECRET задан.
    Значение задаётся в заголовках действия триггера в Яндекс Трекере.
    """
    if not config.WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Tracker-Secret") == config.WEBHOOK_SECRET


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
    Обработчик webhook от триггера Яндекс Трекера.

    Ожидаемое тело (шаблон в настройках триггера):
        {"issue_key": "{{issue.key}}"}

    Коды ответов:
        200 — успех
        400 — плохой запрос     (Трекер НЕ повторяет)
        403 — неверный секрет   (Трекер НЕ повторяет)
        500 — внутренняя ошибка (Трекер повторит до 5 раз)
        502 — ошибка Трекер API (Трекер НЕ повторяет)
    """
    if not _check_secret():
        logger.warning("Неверный секрет от %s", request.remote_addr)
        abort(403, description="Invalid secret")

    data = request.get_json(silent=True)
    if not data:
        abort(400, description="Пустое тело или не JSON")

    issue_key = data.get("issue_key")
    if not issue_key:
        abort(400, description="Поле \'issue_key\' отсутствует")

    logger.info("Получен webhook: issue_key=%s", issue_key)

    try:
        result = sync_tags_to_subtasks(issue_key)
        return jsonify(result), 200

    except TrackerAPIError as exc:
        logger.error("Ошибка Трекер API: %s", exc)
        return jsonify({"error": str(exc)}), 502

    except Exception as exc:
        logger.exception("Неожиданная ошибка: %s", exc)
        return jsonify({"error": "Internal server error"}), 500


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
