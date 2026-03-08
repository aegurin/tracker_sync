"""
Клиент Яндекс Трекер API v3.

Поддерживает наследование от задачи ЛЮБОГО типа к подзадачам:
  - тегов (tags)              — системное поле
  - businessPriority          — локальное пользовательское поле (тип integer)

Локальные поля в Яндекс Трекере хранятся в ответе API под полным ID вида:
  "<queue_id>--<key>"  (например: "66af837b466cdf786c0e0ee6--businessPriority")

При записи через Bulk Change API тоже нужно использовать полный ID.
"""
import time

import requests

import config
from logger import get_logger

logger = get_logger(__name__)

BULKCHANGE_POLL_INTERVAL = 2   # сек между запросами статуса
BULKCHANGE_POLL_TIMEOUT  = 60  # максимальное время ожидания, сек

# ──────────────────────────────────────────────────────────────
# Маппинг локальных полей: короткий ключ → полный ID в API
# Полный ID берётся из атрибута "id" поля в ответе API.
# ──────────────────────────────────────────────────────────────
LOCAL_FIELDS: dict[str, str] = {
    "businessPriority": "66af837b466cdf786c0e0ee6--businessPriority",
}


def _api_field_key(field_key: str) -> str:
    """
    Вернуть ключ поля для использования в API-запросах.
    Локальные поля подменяются на полный ID.

    Пример:
        "tags"             → "tags"
        "businessPriority" → "66af837b466cdf786c0e0ee6--businessPriority"
    """
    return LOCAL_FIELDS.get(field_key, field_key)


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
    GET /v3/issues/{issue_key}
    Локальные поля (LOCAL_FIELDS) запрашиваются явно через ?fields=
    иначе API их не возвращает по умолчанию.
    """
    params = {}
    if LOCAL_FIELDS:
        # Запросить все локальные поля по их полному ID
        params["fields"] = ",".join(LOCAL_FIELDS.values())
    return _request("GET", f"/issues/{issue_key}", params=params)


def get_issue_tags(issue_key: str) -> list[str]:
    """Получить список тегов задачи."""
    tags = get_issue(issue_key).get("tags") or []
    logger.debug("Теги [%s]: %s", issue_key, tags)
    return tags


def get_issue_field(issue_key: str, field_key: str):
    """
    Получить значение поля задачи по короткому ключу.

    Автоматически подменяет короткий ключ на полный ID для локальных полей.

    Args:
        issue_key: ключ задачи (например PROJ-1)
        field_key: короткий ключ поля (например businessPriority)

    Returns:
        значение поля или None если поле отсутствует
    """
    issue = get_issue(issue_key)
    api_key = _api_field_key(field_key)
    value = issue.get(api_key)
    logger.debug("Поле [%s].%s (api_key=%s) = %s", issue_key, field_key, api_key, value)
    return value


def get_issue_fields(issue_key: str, field_keys: list[str]) -> dict:
    """
    Получить значения нескольких полей задачи за один запрос.

    Для локальных полей автоматически использует полный ID в API-ответе,
    но возвращает результат под коротким ключом.

    Args:
        issue_key:  ключ задачи
        field_keys: список коротких ключей полей

    Returns:
        {short_key: value} — всегда под коротким ключом

    Пример:
        get_issue_fields("PROJ-1", ["tags", "businessPriority"])
        → {"tags": ["backend"], "businessPriority": 3}
    """
    issue = get_issue(issue_key)

    result = {}
    for key in field_keys:
        api_key = _api_field_key(key)
        value = issue.get(api_key)
        result[key] = value
        if value is None:
            logger.warning(
                "Поле '%s' (api_key='%s') не найдено в задаче %s. "
                "Доступные ключи: %s",
                key, api_key, issue_key,
                [k for k in issue.keys() if key.lower() in k.lower()] or "не найдено",
            )

    logger.debug("Поля [%s]: %s", issue_key, result)
    return result


def get_subtasks(parent_key: str) -> list[str]:
    """
    Получить ключи всех прямых подзадач через API связей.
    GET /v3/issues/{parent_key}/links

    Фильтр: type.id == "subtask" AND direction == "outward"
    Работает для задач ЛЮБОГО типа.
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

    Локальные поля автоматически подменяются на полный ID
    перед отправкой в API.

    Args:
        issue_keys: ключи задач для обновления (до 10 000)
        fields:     {short_key: value}
                    Примеры:
                      {"tags": ["backend", "sprint-10"]}
                      {"businessPriority": 3}
                      {"tags": [...], "businessPriority": 3}
        notify:     уведомлять исполнителей

    Returns:
        объект bulkchange с полями id, status
    """
    # Подменяем короткие ключи на полные API-ключи для локальных полей
    api_fields = {_api_field_key(k): v for k, v in fields.items()}

    logger.info(
        "Bulk update: %d задач → поля %s",
        len(issue_keys), list(fields.keys()),
    )
    logger.debug("API payload values: %s", api_fields)

    return _request("POST", "/bulkchange/_update", json={
        "issues": issue_keys,
        "values": api_fields,
        "notify": notify,
    })


def bulk_update_tags(
    issue_keys: list[str],
    tags: list[str],
    notify: bool = False,
) -> dict:
    """Массово обновить теги. Обёртка над bulk_update_fields."""
    return bulk_update_fields(issue_keys, {"tags": tags}, notify)


def get_bulkchange_status(bulkchange_id: str) -> dict:
    """
    Получить статус задания.
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

    Работает для задачи ЛЮБОГО типа и любых полей (системных и локальных).

    Args:
        parent_key:  ключ родительской задачи
        field_keys:  список коротких ключей полей
                     Примеры: ["tags"], ["businessPriority"], ["tags", "businessPriority"]
        notify:      уведомлять исполнителей

    Returns:
        {
          "parent":        str,
          "fields":        dict,   # {short_key: value}
          "subtasks":      list[str],
          "bulkchange_id": str | None,
          "status":        str,    # DONE | FAILED | SKIPPED
          "error":         str,    # только при ошибке
        }
    """
    logger.info("▶ Синхронизация полей %s: %s", field_keys, parent_key)

    parent_fields = get_issue_fields(parent_key, field_keys)
    logger.info("Значения полей [%s]: %s", parent_key, parent_fields)

    # Фильтруем None — не обновляем незаполненные поля
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
    """Синхронизировать теги → подзадачи."""
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
    Синхронизировать businessPriority → подзадачи.

    businessPriority — локальное поле типа integer.
    В API читается под ключом "66af837b466cdf786c0e0ee6--businessPriority",
    записывается тоже под полным ID через Bulk Change API.
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
    Синхронизировать теги И businessPriority одним bulk-запросом → подзадачи.
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
