"""
Клиент Яндекс Трекер API v3.

Все обновления полей выполняются через PATCH /v3/issues/{key}.
Bulk Change API не используется — он не поддерживает локальные поля.

Для N подзадач запросы выполняются параллельно (ThreadPoolExecutor).

Локальные поля (например businessPriority) хранятся в API-ответе
под полным ID вида "<queue_id>--<key>". Маппинг задаётся в LOCAL_FIELDS.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config
from logger import get_logger

logger = get_logger(__name__)

PATCH_MAX_WORKERS = 5  # параллельных PATCH-запросов

# ──────────────────────────────────────────────────────────────
# Локальные поля: короткий ключ → полный ID в API
# Полный ID берётся из: GET /v3/queues/{queue}/localFields → поле "id"
# ──────────────────────────────────────────────────────────────
LOCAL_FIELDS: dict[str, str] = {
    "businessPriority": "66af837b466cdf786c0e0ee6--businessPriority",
}


def _api_key(field_key: str) -> str:
    """
    Вернуть ключ поля для использования в API (чтение и запись).
    Локальные поля подменяются на полный ID.

        "tags"             → "tags"
        "businessPriority" → "66af837b466cdf786c0e0ee6--businessPriority"
    """
    return LOCAL_FIELDS.get(field_key, field_key)


# ──────────────────────────────────────────────────────────────

class TrackerAPIError(Exception):
    """Ошибка HTTP-запроса к Яндекс Трекер API."""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {body}")


def _request(method: str, path: str, **kwargs) -> dict | list:
    """Базовый HTTP-запрос к API. Возвращает JSON или бросает TrackerAPIError."""
    url = f"{config.TRACKER_API_BASE}{path}"
    logger.debug("%s %s", method, url)
    resp = requests.request(
        method, url,
        headers=config.get_headers(),
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        logger.error("API %s %s → %d %s", method, path, resp.status_code, resp.text[:300])
        raise TrackerAPIError(resp.status_code, resp.text)
    return resp.json()


# ──────────────────────────────────────────────────────────────
# Чтение задач
# ──────────────────────────────────────────────────────────────

def get_issue(issue_key: str) -> dict:
    """
    GET /v3/issues/{issue_key}

    Локальные поля явно запрашиваются через ?fields=<id1>,<id2>
    — без этого API их не возвращает.
    """
    params = {}
    if LOCAL_FIELDS:
        params["fields"] = ",".join(LOCAL_FIELDS.values())
    return _request("GET", f"/issues/{issue_key}", params=params)


def get_issue_fields(issue_key: str, field_keys: list[str]) -> dict:
    """
    Получить значения полей задачи за один запрос.

    Локальные поля читаются по полному ID, но возвращаются
    под коротким ключом для единообразия.

    Args:
        issue_key:  ключ задачи
        field_keys: короткие ключи полей, например ["tags", "businessPriority"]

    Returns:
        {"tags": [...], "businessPriority": 900}
    """
    issue = get_issue(issue_key)
    result = {}
    for key in field_keys:
        value = issue.get(_api_key(key))
        result[key] = value
        if value is None:
            similar = [k for k in issue if key.lower() in k.lower()]
            logger.warning(
                "Поле '%s' (api_key='%s') не найдено в %s. Похожие: %s",
                key, _api_key(key), issue_key, similar or "—",
            )
    logger.debug("Поля [%s]: %s", issue_key, result)
    return result


def get_subtasks(parent_key: str) -> list[str]:
    """
    GET /v3/issues/{parent_key}/links

    Фильтр: type.id == "subtask" AND direction == "outward"
    Работает для задач любого типа (Эпик, История, Задача, Баг).
    """
    links = _request("GET", f"/issues/{parent_key}/links")
    keys = [
        lnk["object"]["key"]
        for lnk in links
        if (
            lnk.get("type", {}).get("id") == "subtask"
            and lnk.get("direction") == "outward"
            and lnk.get("object", {}).get("key")
        )
    ]
    logger.info("Подзадачи [%s]: %d → %s", parent_key, len(keys), keys)
    return keys


# ──────────────────────────────────────────────────────────────
# Обновление задач через PATCH
# ──────────────────────────────────────────────────────────────

def patch_issue(issue_key: str, fields: dict) -> dict:
    """
    PATCH /v3/issues/{issue_key}

    Обновляет одну задачу. Короткие ключи локальных полей
    автоматически подменяются на полные ID.

    Args:
        issue_key: ключ задачи
        fields:    {"tags": [...], "businessPriority": 900}
    """
    payload = {_api_key(k): v for k, v in fields.items()}
    logger.debug("PATCH [%s] payload=%s", issue_key, payload)
    return _request("PATCH", f"/issues/{issue_key}", json=payload)


def patch_issues_parallel(
    issue_keys: list[str],
    fields: dict,
    max_workers: int = PATCH_MAX_WORKERS,
) -> dict:
    """
    Параллельно обновить несколько задач через PATCH.

    Запускает до max_workers одновременных HTTP-запросов.
    Ошибки отдельных задач не прерывают обработку остальных.

    Args:
        issue_keys:  список ключей задач
        fields:      поля для обновления (короткие ключи)
        max_workers: число параллельных запросов

    Returns:
        {
          "updated": list[str],   # успешно обновлённые ключи
          "errors":  list[dict],  # [{"issue": key, "error": str}, ...]
        }
    """
    updated: list[str] = []
    errors:  list[dict] = []

    logger.info(
        "PATCH ×%d (workers=%d): поля %s",
        len(issue_keys), max_workers, list(fields.keys()),
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(patch_issue, key, fields): key for key in issue_keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                future.result()
                updated.append(key)
                logger.info("PATCH ✓ [%s]", key)
            except TrackerAPIError as exc:
                errors.append({"issue": key, "error": str(exc)})
                logger.error("PATCH ✗ [%s]: %s", key, exc)

    logger.info(
        "PATCH завершён: ✓%d ✗%d из %d",
        len(updated), len(errors), len(issue_keys),
    )
    return {"updated": updated, "errors": errors}


# ──────────────────────────────────────────────────────────────
# Синхронизация
# ──────────────────────────────────────────────────────────────

def sync_fields_to_subtasks(
    parent_key: str,
    field_keys: list[str],
) -> dict:
    """
    Скопировать поля родительской задачи во все прямые подзадачи.

    Читает значения полей с родителя, затем применяет их ко всем
    подзадачам через параллельные PATCH-запросы.

    Args:
        parent_key: ключ родительской задачи
        field_keys: поля для синхронизации, например ["tags", "businessPriority"]

    Returns:
        {
          "parent":   str,
          "fields":   dict,       # значения полей родителя
          "subtasks": list[str],
          "updated":  list[str],  # успешно обновлённые подзадачи
          "errors":   list[dict], # [] при полном успехе
          "status":   str,        # DONE | PARTIAL | FAILED | SKIPPED
        }
    """
    logger.info("▶ Синхронизация %s → %s", field_keys, parent_key)

    parent_fields = get_issue_fields(parent_key, field_keys)
    logger.info("Значения [%s]: %s", parent_key, parent_fields)

    fields_to_sync = {k: v for k, v in parent_fields.items() if v is not None}
    if not fields_to_sync:
        logger.warning("Все поля %s у %s пусты — пропускаем", field_keys, parent_key)
        return _result(parent_key, parent_fields, [], [], [], "SKIPPED")

    subtasks = get_subtasks(parent_key)
    if not subtasks:
        logger.info("У %s нет подзадач — пропускаем", parent_key)
        return _result(parent_key, parent_fields, [], [], [], "SKIPPED")

    patch_result = patch_issues_parallel(subtasks, fields_to_sync)
    updated = patch_result["updated"]
    errors  = patch_result["errors"]

    if not errors:
        status = "DONE"
    elif updated:
        status = "PARTIAL"
    else:
        status = "FAILED"

    logger.info(
        "◀ Готово [%s]: %s → %d подзадач, статус=%s, ошибок=%d",
        parent_key, list(fields_to_sync.keys()), len(subtasks), status, len(errors),
    )
    return _result(parent_key, parent_fields, subtasks, updated, errors, status)


def _result(parent, fields, subtasks, updated, errors, status) -> dict:
    """Сформировать стандартный словарь ответа."""
    return {
        "parent":   parent,
        "fields":   fields,
        "subtasks": subtasks,
        "updated":  updated,
        "errors":   errors,
        "status":   status,
    }


# ──────────────────────────────────────────────────────────────
# Публичные обёртки
# ──────────────────────────────────────────────────────────────

def sync_tags_to_subtasks(parent_key: str) -> dict:
    """Синхронизировать теги → подзадачи."""
    r = sync_fields_to_subtasks(parent_key, ["tags"])
    return {
        "parent":   r["parent"],
        "tags":     r["fields"].get("tags") or [],
        "subtasks": r["subtasks"],
        "updated":  r["updated"],
        "errors":   r["errors"],
        "status":   r["status"],
    }


def sync_business_priority_to_subtasks(parent_key: str) -> dict:
    """
    Синхронизировать businessPriority → подзадачи.

    Локальное поле типа integer (например 900).
    Записывается через PATCH с полным ID поля.
    """
    r = sync_fields_to_subtasks(parent_key, ["businessPriority"])
    return {
        "parent":           r["parent"],
        "businessPriority": r["fields"].get("businessPriority"),
        "subtasks":         r["subtasks"],
        "updated":          r["updated"],
        "errors":           r["errors"],
        "status":           r["status"],
    }


def sync_all_fields_to_subtasks(parent_key: str) -> dict:
    """
    Синхронизировать теги И businessPriority → подзадачи.
    Один вызов — все поля одновременно в каждом PATCH-запросе.
    """
    logger.info("▶ Полная синхронизация [tags + businessPriority]: %s", parent_key)
    r = sync_fields_to_subtasks(parent_key, ["tags", "businessPriority"])
    return {
        "parent":           r["parent"],
        "tags":             r["fields"].get("tags") or [],
        "businessPriority": r["fields"].get("businessPriority"),
        "subtasks":         r["subtasks"],
        "updated":          r["updated"],
        "errors":           r["errors"],
        "status":           r["status"],
    }
