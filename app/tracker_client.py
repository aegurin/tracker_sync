"""
Клиент Яндекс Трекер API v3.

Все обновления полей выполняются через PATCH /v3/issues/{key}.
Запросы для N подзадач выполняются параллельно (ThreadPoolExecutor).

Локальные поля (например businessPriority) хранятся в API-ответе
под полным ID вида "{hash}--{fieldKey}". Маппинг задаётся в QUEUE_LOCAL_FIELDS
отдельно для каждой очереди, так как одно и то же поле имеет разные ID
в разных очередях.

ВАЖНО: параметр ?fields= в GET /v3/issues/{key} ЗАМЕНЯЕТ дефолтный набор
полей ответа. Поэтому при запросе локальных полей нужно явно включать
в ?fields= и системные поля (tags и т.д.), иначе они не вернутся.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config
from logger import get_logger

logger = get_logger(__name__)

PATCH_MAX_WORKERS = 5  # параллельных PATCH-запросов


def _queue_of(issue_key: str) -> str:
    """'ENGINEERINGTEAM-3701' → 'ENGINEERINGTEAM'"""
    return issue_key.split("-")[0].upper()


def _api_key(field_key: str, issue_key: str) -> str:
    """
    Вернуть API-идентификатор поля для конкретной задачи.

    Системные поля (tags, summary и т.д.) возвращаются без изменений.
    Локальные поля подставляются по очереди задачи.

    Примеры:
        _api_key("tags",             "ANY-1")              → "tags"
        _api_key("businessPriority", "BACKENDTEAM-7")      → "66af837b...--businessPriority"
        _api_key("businessPriority", "ENGINEERINGTEAM-3701") → "66af8412...--businessPriority"
    """
    queue = _queue_of(issue_key)
    return config.QUEUE_LOCAL_FIELDS.get(queue, {}).get(field_key, field_key)


def _has_local_fields(field_keys: list[str], issue_key: str) -> bool:
    """Есть ли среди запрошенных полей хотя бы одно локальное для данной очереди?"""
    queue = _queue_of(issue_key)
    queue_fields = config.QUEUE_LOCAL_FIELDS.get(queue, {})
    return any(k in queue_fields for k in field_keys)


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции фильтрации по очереди (для кросс-синхронизации)
# ──────────────────────────────────────────────────────────────────────────────

def _is_queue_allowed(issue_key: str) -> bool:
    """
    Проверить, разрешена ли очередь задачи для кросс-синхронизации.

    Если config.BLOCKER_ALLOWED_QUEUES пуст — разрешены все очереди.
    Иначе — только те, чей префикс есть в списке.

    Examples:
        'BACKENDTEAM-7'      → True   (если BLOCKER_ALLOWED_QUEUES=['BACKENDTEAM'])
        'INFRA-3'            → False  (INFRA не в списке)
        'BACKENDTEAM-7'      → True   (если BLOCKER_ALLOWED_QUEUES=[] — всё разрешено)
    """
    if not config.BLOCKER_ALLOWED_QUEUES:
        return True
    queue = _queue_of(issue_key)
    allowed = queue in config.BLOCKER_ALLOWED_QUEUES
    if not allowed:
        logger.debug(
            "Очередь '%s' не в BLOCKER_ALLOWED_QUEUES=%s — пропускаем",
            queue, config.BLOCKER_ALLOWED_QUEUES,
        )
    return allowed


# ──────────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────────
# Чтение задач
# ──────────────────────────────────────────────────────────────────────────────

def get_issue(issue_key: str, api_keys: list[str] | None = None) -> dict:
    """
    GET /v3/issues/{issue_key}

    api_keys — список API-ключей для параметра ?fields=
    Если передан, API вернёт ТОЛЬКО эти поля (дефолтный набор заменяется).
    Если None — API вернёт стандартный набор полей (без локальных).

    Правило:
      - Нужны только системные поля  → api_keys=None (дефолтный ответ)
      - Нужны локальные поля          → api_keys=[..., "66af837b..."]
      - Нужны оба типа                → api_keys=["tags", "66af837b..."]
    """
    params = {}
    if api_keys:
        params["fields"] = ",".join(api_keys)
    return _request("GET", f"/issues/{issue_key}", params=params)


def get_issue_fields(issue_key: str, field_keys: list[str]) -> dict:
    """
    Получить значения полей задачи за один запрос.

    Корректно строит ?fields= с учётом очереди задачи:
      - Если среди field_keys есть локальные поля → передаём ?fields=
        со ВСЕМИ запрошенными ключами (и системными и локальными)
      - Если только системные → обычно дефолтный ответ; если среди них есть tags,
        явно передаём ?fields=… — иначе пустые теги часто отсутствуют в JSON.

    Локальные поля читаются по полному ID (очередь-специфичному),
    возвращаются под коротким ключом.

    Args:
        issue_key:  ключ задачи
        field_keys: короткие ключи полей, например ["tags", "businessPriority"]

    Returns:
        {"tags": [...], "businessPriority": 900}
    """
    if _has_local_fields(field_keys, issue_key):
        # Явно запрашиваем все поля — и системные и локальные
        # (иначе ?fields=localId заменит дефолтный набор и системные пропадут)
        api_keys_to_request = [_api_key(k, issue_key) for k in field_keys]
        issue = get_issue(issue_key, api_keys=api_keys_to_request)
        logger.debug("GET [%s] ?fields=%s", issue_key, ",".join(api_keys_to_request))
    else:
        # Без ?fields= Трекер нередко не кладёт ключ tags, если тегов нет
        # (как в логе: «Поле tags не найдено» → None → старый SKIPPED).
        if "tags" in field_keys:
            api_keys_to_request = [_api_key(k, issue_key) for k in field_keys]
            issue = get_issue(issue_key, api_keys=api_keys_to_request)
            logger.debug("GET [%s] ?fields=%s", issue_key, ",".join(api_keys_to_request))
        else:
            issue = get_issue(issue_key)
            logger.debug("GET [%s] (default fields)", issue_key)

    result = {}
    for key in field_keys:
        a_key = _api_key(key, issue_key)
        value = issue.get(a_key)
        # Пустые теги: ключ может отсутствовать в JSON → [] для единообразия с PATCH
        if key == "tags" and value is None:
            value = []
        result[key] = value
        if value is None:
            similar = [k for k in issue if key.lower() in k.lower()]
            logger.warning(
                "Поле '%s' (api_key='%s') не найдено в %s. Похожие: %s",
                key, a_key, issue_key, similar or "—",
            )

    logger.debug("Поля [%s]: %s", issue_key, result)
    return result


def get_subtasks(parent_key: str) -> list[str]:
    """
    GET /v3/issues/{parent_key}/links

    Фильтр: type.id == "subtask" AND direction == "outward"
    Работает для задач любого типа.
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


def has_parent(issue_key: str) -> bool:
    """
    Проверить, является ли задача подзадачей (имеет ли родителя).
    Используется для защиты от каскадных вызовов триггера:
    когда PATCH на подзадаче провоцирует повторный webhook.
    """
    issue = get_issue(issue_key)
    return "parent" in issue and issue["parent"] is not None


def get_blocker_issues(issue_key: str) -> list[str]:
    """
    GET /v3/issues/{issue_key}/links

    Ищет задачи, которые БЛОКИРУЮТ issue_key (т.е. issue_key зависит от них).

    Реальная структура ответа Трекера (проверено на ENGINEERINGTEAM-3701):
      type.id   = "depends"   — тип связи «зависимость»
      direction = "outward"   — «текущая задача ЗАВИСИТ ОТ объекта»
                              = объект является блокером текущей задачи

    БЫЛО (неверно):  type.id == "blocks"  AND direction == "inward"
    СТАЛО (верно):   type.id == "depends" AND direction == "outward"
    """
    links = _request("GET", f"/issues/{issue_key}/links")

    if config.LOG_LINKS_RAW:
        logger.debug("RAW links [%s]: %s", issue_key, links)

    all_blockers = [
        lnk["object"]["key"]
        for lnk in links
        if (
            lnk.get("type", {}).get("id") == "depends"
            and lnk.get("direction") == "outward"
            and lnk.get("object", {}).get("key")
        )
    ]

    allowed_blockers = [k for k in all_blockers if _is_queue_allowed(k)]
    skipped = set(all_blockers) - set(allowed_blockers)

    if skipped:
        logger.warning(
            "Блокирующие задачи пропущены (очередь не в BLOCKER_ALLOWED_QUEUES=%s): %s",
            config.BLOCKER_ALLOWED_QUEUES, sorted(skipped),
        )

    logger.info(
        "Блокирующие задачи [%s]: всего=%d, разрешено=%d → %s",
        issue_key, len(all_blockers), len(allowed_blockers), allowed_blockers,
    )
    return allowed_blockers


# ──────────────────────────────────────────────────────────────────────────────
# Обновление задач через PATCH
# ──────────────────────────────────────────────────────────────────────────────

def patch_issue(issue_key: str, fields: dict) -> dict:
    """
    PATCH /v3/issues/{issue_key}

    fields — { короткий_ключ: значение }, например:
        {"tags": ["backend"], "businessPriority": 900}

    API-идентификаторы полей подставляются автоматически
    по очереди issue_key из QUEUE_LOCAL_FIELDS.

    В режиме DRY_RUN=true логирует payload, но НЕ отправляет запрос.
    """
    payload = {_api_key(k, issue_key): v for k, v in fields.items()}

    if config.DRY_RUN:
        logger.info(
            "DRY_RUN: PATCH [%s] НЕ выполнен, payload=%s",
            issue_key, payload,
        )
        return {"key": issue_key, "dry_run": True}

    logger.debug("PATCH [%s] payload=%s", issue_key, payload)
    return _request("PATCH", f"/issues/{issue_key}", json=payload)


def patch_issues_parallel(
    issue_keys: list[str],
    fields: dict,
    max_workers: int = PATCH_MAX_WORKERS,
) -> dict:
    """
    Параллельно обновить несколько задач через PATCH.

    Каждая задача получает payload с API-ключами своей очереди
    (вычисляются внутри patch_issue).

    Ошибки отдельных задач не прерывают обработку остальных.

    Returns:
        {
            "updated": list[str],        # успешно обновлённые ключи
            "errors":  list[dict],       # [{"issue": key, "error": str}, ...]
        }
    """
    mode = "DRY_RUN" if config.DRY_RUN else "LIVE"
    logger.info(
        "PATCH ×%d (workers=%d, mode=%s): поля %s",
        len(issue_keys), max_workers, mode, list(fields.keys()),
    )

    updated: list[str] = []
    errors: list[dict] = []

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

    logger.info("PATCH завершён: ✓%d ✗%d из %d", len(updated), len(errors), len(issue_keys))
    return {"updated": updated, "errors": errors}


# ──────────────────────────────────────────────────────────────────────────────
# Синхронизация: родитель → подзадачи
# ──────────────────────────────────────────────────────────────────────────────


def _fields_to_sync_from_parent(parent_fields: dict, field_keys: list[str]) -> dict:
    """
    Собрать payload для PATCH дочерних/блокирующих задач по значениям источника.

    Важно для тегов: после очистки поля в UI Трекер в GET часто приходит tags=null
    или поле отсутствует. Раньше такие значения отбрасывались (if v is not None),
    из‑за чего fields_to_sync оказывался пустым и синхронизация шла в SKIPPED —
    подзадачи сохраняли старые теги. None для tags трактуем как [].
    """
    out: dict = {}
    for k in field_keys:
        v = parent_fields.get(k)
        if k == "tags":
            if v is None:
                out[k] = []
            elif isinstance(v, list):
                out[k] = v
            else:
                out[k] = [v] if v else []
        elif v is not None:
            out[k] = v
    return out


def sync_fields_to_subtasks(parent_key: str, field_keys: list[str]) -> dict:
    """
    Скопировать поля родительской задачи во все прямые подзадачи.

    Защита от каскада: если задача сама является подзадачей (имеет parent),
    синхронизация не выполняется → статус SKIPPED.

    Args:
        parent_key: ключ задачи-родителя
        field_keys: поля для синхронизации, например ["tags", "businessPriority"]

    Returns:
        {parent, fields, subtasks, updated, errors,
         status: DONE|PARTIAL|FAILED|SKIPPED}
    """
    logger.info("▶ Синхронизация %s → %s", field_keys, parent_key)

    if has_parent(parent_key):
        logger.info("Задача %s имеет родителя — пропускаем (защита от каскада)", parent_key)
        return _result(parent_key, {}, [], [], [], "SKIPPED")

    parent_fields = get_issue_fields(parent_key, field_keys)
    logger.info("Значения [%s]: %s", parent_key, parent_fields)

    fields_to_sync = _fields_to_sync_from_parent(parent_fields, field_keys)
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
    status  = "DONE" if not errors else ("PARTIAL" if updated else "FAILED")

    logger.info(
        "◀ Готово [%s]: %s → %d подзадач, статус=%s, ошибок=%d",
        parent_key, list(fields_to_sync.keys()), len(subtasks), status, len(errors),
    )
    return _result(parent_key, parent_fields, subtasks, updated, errors, status)


# ──────────────────────────────────────────────────────────────────────────────
# Синхронизация: источник → блокирующие задачи (кросс-очередная)
# ──────────────────────────────────────────────────────────────────────────────

def sync_fields_to_blockers(source_key: str, field_keys: list[str]) -> dict:
    """
    Скопировать поля эпика-источника во все блокирующие эпики.

    Используется для кросс-очередной передачи:
      ENGINEERINGTEAM-epic → BACKENDTEAM-epic (через связь "depends" outward).

    Читает значения полей с API-ключами очереди источника (ENGINEERINGTEAM).
    Пишет значения с API-ключами очереди получателя (BACKENDTEAM).
    Каждый вызов patch_issue сам подставляет правильный ID для своей очереди.

    Args:
        source_key: ключ задачи-источника (напр. "ENGINEERINGTEAM-3701")
        field_keys: поля для синхронизации ["tags", "businessPriority"]

    Returns:
        стандартный словарь результата (аналогично sync_fields_to_subtasks)
    """
    logger.info("▶ Кросс-очередная синхронизация [%s] → блокирующие задачи", source_key)

    source_fields = get_issue_fields(source_key, field_keys)
    logger.info("Значения источника [%s]: %s", source_key, source_fields)

    fields_to_sync = _fields_to_sync_from_parent(source_fields, field_keys)
    if not fields_to_sync:
        logger.warning("Все поля %s у %s пусты — пропускаем", field_keys, source_key)
        return _result(source_key, source_fields, [], [], [], "SKIPPED")

    blockers = get_blocker_issues(source_key)
    if not blockers:
        logger.info("У %s нет блокирующих задач — пропускаем", source_key)
        return _result(source_key, source_fields, [], [], [], "SKIPPED")

    patch_result = patch_issues_parallel(blockers, fields_to_sync)
    updated = patch_result["updated"]
    errors  = patch_result["errors"]
    status  = "DONE" if not errors else ("PARTIAL" if updated else "FAILED")

    logger.info(
        "◀ Кросс-синхронизация [%s]: %s → %d блокирующих задач, статус=%s",
        source_key, list(fields_to_sync.keys()), len(blockers), status,
    )
    return _result(source_key, source_fields, blockers, updated, errors, status)


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательная функция результата
# ──────────────────────────────────────────────────────────────────────────────

def _result(parent, fields, subtasks, updated, errors, status) -> dict:
    return {
        "parent":   parent,
        "fields":   fields,
        "subtasks": subtasks,
        "updated":  updated,
        "errors":   errors,
        "status":   status,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Публичные обёртки для webhook_server.py
# ──────────────────────────────────────────────────────────────────────────────

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
    Локальное поле типа integer. Записывается через PATCH с полным ID поля.
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
    Синхронизировать теги И businessPriority → подзадачи одним вызовом.
    Каждый PATCH-запрос обновляет оба поля одновременно.
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


def sync_tags_to_blockers(source_key: str) -> dict:
    """
    Синхронизировать ТОЛЬКО теги ENG-эпика → блокирующие BACKEND-эпики.
    Триггер: поле tags изменилось на эпике очереди Engineering.
    """
    r = sync_fields_to_blockers(source_key, ["tags"])
    return {
        "source":   r["parent"],
        "tags":     r["fields"].get("tags") or [],
        "blockers": r["subtasks"],
        "updated":  r["updated"],
        "errors":   r["errors"],
        "status":   r["status"],
    }


def sync_business_priority_to_blockers(source_key: str) -> dict:
    """
    Синхронизировать ТОЛЬКО businessPriority ENG-эпика → блокирующие BACKEND-эпики.
    Триггер: поле businessPriority изменилось на эпике очереди Engineering.

    Чтение:  ENGINEERINGTEAM → 66af8412375a31188f658397--businessPriority
    Запись:  BACKENDTEAM     → 66af837b466cdf786c0e0ee6--businessPriority
    """
    r = sync_fields_to_blockers(source_key, ["businessPriority"])
    return {
        "source":           r["parent"],
        "businessPriority": r["fields"].get("businessPriority"),
        "blockers":         r["subtasks"],
        "updated":          r["updated"],
        "errors":           r["errors"],
        "status":           r["status"],
    }


def sync_all_fields_to_blockers(source_key: str) -> dict:
    """Синхронизировать tags + businessPriority из source_key во все блокирующие задачи."""
    logger.info("▶ Полная кросс-синхронизация [tags + businessPriority]: %s", source_key)
    r = sync_fields_to_blockers(source_key, ["tags", "businessPriority"])
    return {
        "source":           r["parent"],
        "tags":             r["fields"].get("tags") or [],
        "businessPriority": r["fields"].get("businessPriority"),
        "blockers":         r["subtasks"],
        "updated":          r["updated"],
        "errors":           r["errors"],
        "status":           r["status"],
    }