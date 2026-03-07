"""
Утилита разовой синхронизации тегов ВСЕХ задач очереди с их подзадачами.

Обходит все задачи с тегами в очереди, у которых есть подзадачи,
и синхронизирует теги. Работает для задач ЛЮБОГО типа.

Использование:
    python sync_all.py --queue MYPROJECT
    python sync_all.py --queue MYPROJECT --dry-run
    python sync_all.py --queue MYPROJECT --type epic
    python sync_all.py --queue MYPROJECT --type epic --type story
    python sync_all.py --queue MYPROJECT --type task --type bug
"""
import argparse
import logging
import sys

import requests

import config
from tracker_client import sync_tags_to_subtasks, get_subtasks, TrackerAPIError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
)
logger = logging.getLogger(__name__)


def search_issues(queue_key: str, issue_types: list[str] | None = None) -> list[dict]:
    """
    Получить задачи с тегами из очереди постранично.
    POST /v3/issues/_search

    Args:
        queue_key:   ключ очереди
        issue_types: список типов задач (None = все типы)
    """
    url = f"{config.TRACKER_API_BASE}/issues/_search"

    query_filter: dict = {
        "queue": queue_key,
        "tags": {"exists": True},
    }
    if issue_types:
        query_filter["type"] = issue_types

    payload = {
        "filter": query_filter,
        "fields": ["key", "summary", "type", "tags"],
    }

    issues, page = [], 1
    while True:
        resp = requests.post(
            url,
            headers=config.get_headers(),
            json=payload,
            params={"perPage": 100, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        issues.extend(batch)
        if len(batch) < 100:
            break
        page += 1

    return issues


def main():
    parser = argparse.ArgumentParser(
        description="Синхронизация тегов задач → подзадачи (любой тип)"
    )
    parser.add_argument(
        "--queue", required=True,
        help="Ключ очереди (например: MYPROJECT)"
    )
    parser.add_argument(
        "--type", dest="types", action="append", metavar="TYPE",
        help=(
            "Фильтр по типу задачи (можно указать несколько раз). "
            "Не указывать = все типы. "
            "Примеры: epic, story, task, bug"
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Показать план без применения изменений"
    )
    args = parser.parse_args()

    if not config.TRACKER_TOKEN:
        logger.error("TRACKER_TOKEN не задан в .env")
        sys.exit(1)

    type_label = f"типов: {args.types}" if args.types else "всех типов"
    logger.info(
        "Поиск задач с тегами в очереди \'%s\' (%s)...",
        args.queue, type_label
    )

    if args.dry_run:
        logger.info("🔍 Режим dry-run: изменения НЕ применяются")

    try:
        issues = search_issues(args.queue, args.types)
    except requests.HTTPError as exc:
        logger.error("Не удалось получить задачи: %s", exc)
        sys.exit(1)

    if not issues:
        logger.info("Задачи с тегами не найдены в очереди \'%s\'", args.queue)
        return

    logger.info("Найдено задач с тегами: %d — проверяем подзадачи...\n", len(issues))

    total_synced = total_skipped = total_errors = 0

    for issue in issues:
        key       = issue["key"]
        tags      = issue.get("tags") or []
        type_info = issue.get("type", {})
        type_name = type_info.get("display", type_info.get("id", "unknown"))

        logger.info("→ [%s] %s  теги: %s", type_name, key, tags)

        if args.dry_run:
            subtasks = get_subtasks(key)
            if subtasks:
                logger.info("  [dry-run] подзадачи: %s → получат теги: %s", subtasks, tags)
            else:
                logger.info("  [dry-run] подзадач нет — пропустим")
            continue

        try:
            result = sync_tags_to_subtasks(key)
            if result["status"] == "SKIPPED":
                total_skipped += 1
            else:
                total_synced += 1
        except TrackerAPIError as exc:
            logger.error("  ✗ Ошибка: %s", exc)
            total_errors += 1

    if not args.dry_run:
        logger.info(
            "\n✅ Итого: обработано=%d  синхронизировано=%d  "
            "пропущено (нет подзадач)=%d  ошибок=%d",
            len(issues), total_synced, total_skipped, total_errors,
        )


if __name__ == "__main__":
    main()
