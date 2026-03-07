"""
Клиент Яндекс Трекер API v3.

Поддерживает наследование тегов от задачи ЛЮБОГО типа
(Эпик, История, Задача, Баг и т.д.) ко всем её прямым подзадачам.
Использует Bulk Change API для обновления всех подзадач одним запросом.

Документация: https://yandex.ru/support/tracker/ru/about-api
"""
import logging
import time

import requests

import config

logger = logging.getLogger(__name__)

BULKCHANGE_POLL_INTERVAL = 2   # сек между запросами статуса
BULKCHANGE_POLL_TIMEOUT  = 60  # максимальное время ожидания, сек


class TrackerAPIError(Exception):
    """Ошибка при обращении к Яндекс Трекер API."""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {body}")


def _request(method: str, path: str, **kwargs) -> dict | list:
    """Выполнить HTTP-запрос к Трекер API и вернуть JSON."""
    url = f"{config.TRACKER_API_BASE}{path}"
    resp = requests.request(
        method,
        url,
        headers=config.get_headers(),
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        raise TrackerAPIError(resp.status_code, resp.text)
    return resp.json()


# ──────────────────────────────────────────────────────────────
# Операции с задачами
# ──────────────────────────────────────────────────────────────

def get_issue(issue_key: str) -> dict:
    """
    Получить данные задачи.
    GET /v3/issues/{issue_key}
    """
    return _request("GET", f"/issues/{issue_key}")


def get_issue_tags(issue_key: str) -> list[str]:
    """Получить список тегов задачи."""
    return get_issue(issue_key).get("tags") or []


def get_subtasks(parent_key: str) -> list[str]:
    """
    Получить ключи всех прямых подзадач через API связей.
    GET /v3/issues/{parent_key}/links

    Фильтр:
      type.id == "subtask" AND direction == "inward"
      → связанная задача является подзадачей parent_key.

    Работает для задачи ЛЮБОГО типа: Эпик, История, Задача, Баг и т.д.
    """
    links = _request("GET", f"/issues/{parent_key}/links")

    subtask_keys = [
        link["object"]["key"]
        for link in links
        if (
            link.get("type", {}).get("id") == "subtask"
            and link.get("direction") == "inward"
            and link.get("object", {}).get("key")
        )
    ]

    logger.info("Задача %s → найдено подзадач: %d %s",
                parent_key, len(subtask_keys), subtask_keys)
    return subtask_keys


# ──────────────────────────────────────────────────────────────
# Bulk Change API
# ──────────────────────────────────────────────────────────────

def bulk_update_tags(
    issue_keys: list[str],
    tags: list[str],
    notify: bool = False,
) -> dict:
    """
    Массово установить теги для списка задач одним запросом.
    POST /v3/bulkchange/_update

    Операция асинхронная — возвращает объект задания (id, status).
    Максимум 10 000 задач за один запрос.

    Args:
        issue_keys: ключи задач для обновления
        tags:       новый полный список тегов (полная замена)
        notify:     уведомлять исполнителей об изменении
    """
    payload = {
        "issues": issue_keys,
        "values": {"tags": tags},
        "notify": notify,
    }
    logger.info("Bulk update: %d задач → теги %s", len(issue_keys), tags)
    return _request("POST", "/bulkchange/_update", json=payload)


def get_bulkchange_status(bulkchange_id: str) -> dict:
    """
    Получить статус задания группового изменения.
    GET /v3/bulkchange/{bulkchange_id}

    Статусы: CREATED → RUNNING → DONE | FAILED
    """
    return _request("GET", f"/bulkchange/{bulkchange_id}")


def wait_for_bulkchange(bulkchange_id: str) -> dict:
    """
    Опрашивать статус задания до завершения (DONE или FAILED).

    Raises:
        TimeoutError    — задание не завершилось за BULKCHANGE_POLL_TIMEOUT сек
        TrackerAPIError — задание завершилось со статусом FAILED
    """
    deadline = time.monotonic() + BULKCHANGE_POLL_TIMEOUT

    while time.monotonic() < deadline:
        job = get_bulkchange_status(bulkchange_id)
        status = job.get("status", "")

        logger.debug("BulkChange %s: status=%s", bulkchange_id, status)

        if status == "DONE":
            logger.info("BulkChange %s завершён успешно", bulkchange_id)
            return job

        if status == "FAILED":
            raise TrackerAPIError(
                0, f"BulkChange {bulkchange_id} завершился с ошибкой: {job}"
            )

        time.sleep(BULKCHANGE_POLL_INTERVAL)

    raise TimeoutError(
        f"BulkChange {bulkchange_id} не завершился за {BULKCHANGE_POLL_TIMEOUT} сек"
    )


# ──────────────────────────────────────────────────────────────
# Основная бизнес-логика
# ──────────────────────────────────────────────────────────────

def sync_tags_to_subtasks(parent_key: str) -> dict:
    """
    Скопировать теги задачи во все её прямые подзадачи.

    Работает для задачи ЛЮБОГО типа: Эпик, История, Задача, Баг и т.д.

    Алгоритм:
        1. Читает актуальные теги родительской задачи.
        2. Получает список подзадач через links API.
        3. Запускает ОДИН bulk update запрос для всех подзадач.
        4. Ожидает завершения асинхронного задания.

    Returns:
        {
          "parent":        str,
          "tags":          list[str],
          "subtasks":      list[str],
          "bulkchange_id": str | None,
          "status":        str,   # DONE | FAILED | SKIPPED
          "error":         str,   # только при ошибке
        }
    """
    logger.info("▶ Синхронизация тегов: %s", parent_key)

    parent_tags = get_issue_tags(parent_key)
    logger.info("  Теги задачи: %s", parent_tags)

    subtasks = get_subtasks(parent_key)
    if not subtasks:
        logger.info("  Подзадач нет — пропускаем")
        return {
            "parent": parent_key,
            "tags": parent_tags,
            "subtasks": [],
            "bulkchange_id": None,
            "status": "SKIPPED",
        }

    job = bulk_update_tags(subtasks, parent_tags, notify=False)
    bulkchange_id = job["id"]
    logger.info("  BulkChange создан: id=%s", bulkchange_id)

    try:
        final = wait_for_bulkchange(bulkchange_id)
        logger.info("◀ Готово: %d подзадач обновлено, bulkchange=%s",
                    len(subtasks), bulkchange_id)
        return {
            "parent": parent_key,
            "tags": parent_tags,
            "subtasks": subtasks,
            "bulkchange_id": bulkchange_id,
            "status": final.get("status", "DONE"),
        }
    except (TimeoutError, TrackerAPIError) as exc:
        logger.error("  ✗ Ошибка BulkChange %s: %s", bulkchange_id, exc)
        return {
            "parent": parent_key,
            "tags": parent_tags,
            "subtasks": subtasks,
            "bulkchange_id": bulkchange_id,
            "status": "FAILED",
            "error": str(exc),
        }
