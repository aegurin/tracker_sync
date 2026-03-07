"""
Клиент Яндекс Трекер API v3.

Поддерживает наследование от задачи ЛЮБОГО типа к подзадачам:
  - тегов (tags)
  - пользовательского поля businessPriority
  - любой комбинации полей через sync_fields_to_subtasks()

Bulk Change API: один запрос на все подзадачи (до 10 000).
"""
import time

import requests

import config
from logger import get_logger

logger = get_logger(__name__)

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
    logger.debug("%s %s", method, url)
    resp = requests.request(
        method,
        url,
        headers=config.get_headers(),
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        logger.error(
            "API error: %s %s → %d %s",
            method, path, resp.status_code, resp.text[:300],
        )
        raise TrackerAPIError(resp.status_code, resp.text)
    logger.debug("%s %s → %d", method, path, resp.status_code)
    return resp.json()


# ──────────────────────────────────────────────────────────────
# Операции с задачами
# ──────────────────────────────────────────────────────────────

def get_issue(issue_key: str) -> dict:
    """
    Получить все данные задачи.
    GET /v3/issues/{issue_key}
    """
    return _request("GET", f"/issues/{issue_key}")


def get_issue_tags(issue_key: str) -> list[str]:
    """Получить список тегов задачи."""
    tags = get_issue(issue_key).get("tags") or []
    logger.debug("Теги [%s]: %s", issue_key, tags)
    return tags


def get_issue_field(issue_key: str, field_key: str):
    """
    Получить значение произвольного поля задачи по ключу.

    Работает как для системных полей (tags, priority),
    так и для пользовательских (businessPriority и др.).

    Args:
        issue_key: ключ задачи (например PROJ-1)
        field_key: ключ поля   (например businessPriority)

    Returns:
        значение поля или None если поле отсутствует
    """
    issue = get_issue(issue_key)
    value = issue.get(field_key)
    logger.debug("Поле [%s].%s = %s", issue_key, field_key, value)
    return value


def get_issue_fields(issue_key: str, field_keys: list[str]) -> dict:
    """
    Получить значения нескольких полей задачи за один запрос.

    Args:
        issue_key:  ключ задачи
        field_keys: список ключей полей

    Returns:
        словарь {field_key: value}, значение None для отсутствующих полей

    Пример:
        get_issue_fields("PROJ-1", ["tags", "businessPriority"])
        → {"tags": ["backend"], "businessPriority": {"id": "high", "display": "Высокий"}}
    """
    issue = get_issue(issue_key)
    result = {key: issue.get(key) for key in field_keys}
    logger.debug("Поля [%s]: %s", issue_key, result)
    return result


def get_subtasks(parent_key: str) -> list[str]:
    """
    Получить ключи всех прямых подзадач через API связей.
    GET /v3/issues/{parent_key}/links

    Фильтр: type.id == "subtask" AND direction == "outward"
    Работает для задач ЛЮБОГО типа: Эпик, История, Задача, Баг и т.д.
    """
    links = _request("GET", f"/issues/{parent_key}/links")

    subtask_keys = [
        link["object"]["key"]
        for link in links
        if (
            link.get("type", {}).get("id") == "subtask"
            and link.get("direction") == "outward"
            and link.get("object", {}).get("key")
        )
    ]

    logger.info("Задача %s → подзадач: %d %s", parent_key, len(subtask_keys), subtask_keys)
    return subtask_keys


# ──────────────────────────────────────────────────────────────
# Bulk Change API
# ──────────────────────────────────────────────────────────────

def bulk_update_fields(
    issue_keys: list[str],
    fields: dict,
    notify: bool = False,
) -> dict:
    """
    Массово обновить произвольные поля задач одним запросом.
    POST /v3/bulkchange/_update

    Операция асинхронная — возвращает объект задания (id, status).
    Максимум 10 000 задач за один запрос.

    Args:
        issue_keys: ключи задач для обновления
        fields:     словарь полей для изменения
                    Примеры:
                      {"tags": ["backend", "sprint-10"]}
                      {"businessPriority": {"id": "high"}}
                      {"tags": [...], "businessPriority": {...}}
        notify:     уведомлять исполнителей об изменении

    Returns:
        объект bulkchange с полями id, status
    """
    logger.info(
        "Bulk update: %d задач → поля %s",
        len(issue_keys), list(fields.keys()),
    )
    return _request("POST", "/bulkchange/_update", json={
        "issues": issue_keys,
        "values": fields,
        "notify": notify,
    })


def bulk_update_tags(
    issue_keys: list[str],
    tags: list[str],
    notify: bool = False,
) -> dict:
    """
    Массово обновить теги задач.
    Обёртка над bulk_update_fields для обратной совместимости.
    """
    return bulk_update_fields(issue_keys, {"tags": tags}, notify)


def get_bulkchange_status(bulkchange_id: str) -> dict:
    """
    Получить статус задания группового изменения.
    GET /v3/bulkchange/{bulkchange_id}

    Статусы: CREATED → RUNNING → DONE | FAILED
    """
    return _request("GET", f"/bulkchange/{bulkchange_id}")


def wait_for_bulkchange(bulkchange_id: str) -> dict:
    """
    Опрашивать статус задания до завершения.

    Raises:
        TimeoutError    — не завершилось за BULKCHANGE_POLL_TIMEOUT сек
        TrackerAPIError — завершилось со статусом FAILED
    """
    deadline = time.monotonic() + BULKCHANGE_POLL_TIMEOUT

    while time.monotonic() < deadline:
        job = get_bulkchange_status(bulkchange_id)
        status = job.get("status", "")
        logger.debug("BulkChange %s: status=%s", bulkchange_id, status)

        if status == "DONE":
            logger.info("BulkChange %s → DONE", bulkchange_id)
            return job

        if status == "FAILED":
            logger.error("BulkChange %s → FAILED: %s", bulkchange_id, job)
            raise TrackerAPIError(0, f"BulkChange {bulkchange_id} FAILED: {job}")

        time.sleep(BULKCHANGE_POLL_INTERVAL)

    raise TimeoutError(
        f"BulkChange {bulkchange_id} не завершился за {BULKCHANGE_POLL_TIMEOUT} сек"
    )


# ──────────────────────────────────────────────────────────────
# Бизнес-логика синхронизации
# ──────────────────────────────────────────────────────────────

def sync_fields_to_subtasks(
    parent_key: str,
    field_keys: list[str],
    notify: bool = False,
) -> dict:
    """
    Универсальная синхронизация произвольных полей родительской задачи
    во все её прямые подзадачи через Bulk Change API.

    Работает для задачи ЛЮБОГО типа.

    Args:
        parent_key:  ключ родительской задачи
        field_keys:  список ключей полей для синхронизации
                     Примеры: ["tags"], ["businessPriority"], ["tags", "businessPriority"]
        notify:      уведомлять исполнителей

    Returns:
        {
          "parent":        str,
          "fields":        dict,   # {field_key: value} актуальные значения
          "subtasks":      list[str],
          "bulkchange_id": str | None,
          "status":        str,    # DONE | FAILED | SKIPPED
          "error":         str,    # только при ошибке
        }
    """
    logger.info("▶ Синхронизация полей %s: %s", field_keys, parent_key)

    # Читаем все нужные поля за один запрос
    parent_fields = get_issue_fields(parent_key, field_keys)
    logger.info("Значения полей [%s]: %s", parent_key, parent_fields)

    # Фильтруем поля с None — не передаём пустые значения
    fields_to_sync = {k: v for k, v in parent_fields.items() if v is not None}
    if not fields_to_sync:
        logger.warning(
            "Все поля %s у задачи %s пусты — пропускаем",
            field_keys, parent_key,
        )
        return {
            "parent": parent_key,
            "fields": parent_fields,
            "subtasks": [],
            "bulkchange_id": None,
            "status": "SKIPPED",
        }

    subtasks = get_subtasks(parent_key)
    if not subtasks:
        logger.info("У %s нет подзадач — пропускаем", parent_key)
        return {
            "parent": parent_key,
            "fields": parent_fields,
            "subtasks": [],
            "bulkchange_id": None,
            "status": "SKIPPED",
        }

    job = bulk_update_fields(subtasks, fields_to_sync, notify)
    bulkchange_id = job["id"]
    logger.info("BulkChange создан: id=%s задач=%d", bulkchange_id, len(subtasks))

    try:
        final = wait_for_bulkchange(bulkchange_id)
        logger.info(
            "◀ Готово [%s]: поля %s → %d подзадач",
            parent_key, list(fields_to_sync.keys()), len(subtasks),
        )
        return {
            "parent": parent_key,
            "fields": parent_fields,
            "subtasks": subtasks,
            "bulkchange_id": bulkchange_id,
            "status": final.get("status", "DONE"),
        }
    except (TimeoutError, TrackerAPIError) as exc:
        logger.error("Ошибка BulkChange [%s]: %s", parent_key, exc)
        return {
            "parent": parent_key,
            "fields": parent_fields,
            "subtasks": subtasks,
            "bulkchange_id": bulkchange_id,
            "status": "FAILED",
            "error": str(exc),
        }


def sync_tags_to_subtasks(parent_key: str) -> dict:
    """
    Скопировать теги родительской задачи во все подзадачи.
    Обёртка над sync_fields_to_subtasks для обратной совместимости.

    Returns:
        {parent, tags, subtasks, bulkchange_id, status[, error]}
    """
    result = sync_fields_to_subtasks(parent_key, ["tags"])
    return {
        "parent":        result["parent"],
        "tags":          result["fields"].get("tags") or [],
        "subtasks":      result["subtasks"],
        "bulkchange_id": result["bulkchange_id"],
        "status":        result["status"],
        **( {"error": result["error"]} if "error" in result else {} ),
    }


def sync_business_priority_to_subtasks(parent_key: str) -> dict:
    """
    Скопировать поле businessPriority родительской задачи во все подзадачи.

    businessPriority — пользовательское поле, значение обычно объект:
      {"id": "high", "display": "Высокий"}

    Returns:
        {parent, businessPriority, subtasks, bulkchange_id, status[, error]}
    """
    result = sync_fields_to_subtasks(parent_key, ["businessPriority"])
    return {
        "parent":           result["parent"],
        "businessPriority": result["fields"].get("businessPriority"),
        "subtasks":         result["subtasks"],
        "bulkchange_id":    result["bulkchange_id"],
        "status":           result["status"],
        **( {"error": result["error"]} if "error" in result else {} ),
    }


def sync_all_fields_to_subtasks(parent_key: str) -> dict:
    """
    Скопировать ВСЕ наследуемые поля родительской задачи во все подзадачи:
      - tags
      - businessPriority

    Один bulk-запрос на все поля сразу.

    Returns:
        {parent, tags, businessPriority, subtasks, bulkchange_id, status[, error]}
    """
    logger.info("▶ Полная синхронизация [tags + businessPriority]: %s", parent_key)
    result = sync_fields_to_subtasks(parent_key, ["tags", "businessPriority"])
    return {
        "parent":           result["parent"],
        "tags":             result["fields"].get("tags") or [],
        "businessPriority": result["fields"].get("businessPriority"),
        "subtasks":         result["subtasks"],
        "bulkchange_id":    result["bulkchange_id"],
        "status":           result["status"],
        **( {"error": result["error"]} if "error" in result else {} ),
    }
