"""
Централизованный модуль логирования для Tags Sync.

Возможности:
  - Вывод в консоль с цветами (уровень, время, модуль)
  - Ротируемый файл логов (по размеру, с архивом)
  - Опциональный JSON-формат для сбора в ELK/Loki
  - Уровень и пути задаются через .env

Использование в любом модуле:
    from logger import get_logger
    logger = get_logger(__name__)
    logger.info("Сообщение")
    logger.error("Ошибка %s", detail)
"""
import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import config

# ──────────────────────────────────────────────────────────────
# Настройки из .env
# ──────────────────────────────────────────────────────────────
LOG_LEVEL       = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE        = os.getenv("LOG_FILE", "logs/app.log")
LOG_MAX_BYTES   = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 МБ
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
LOG_JSON        = os.getenv("LOG_JSON", "false").lower() == "true"


# ──────────────────────────────────────────────────────────────
# ANSI-цвета для консоли
# ──────────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_COLORS = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Green
    "WARNING":  "\033[33m",   # Yellow
    "ERROR":    "\033[31m",   # Red
    "CRITICAL": "\033[41m",   # Red background
}


class _ColorFormatter(logging.Formatter):
    """Форматтер с ANSI-цветами для консоли."""

    FMT = "{color}{bold}{levelname:<8}{reset} {dim}{asctime}{reset}  " \
          "{bold}{name}{reset}  {message}"

    def format(self, record: logging.LogRecord) -> str:
        color  = _COLORS.get(record.levelname, "")
        dim    = "\033[2m"
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, "%H:%M:%S")

        line = self.FMT.format(
            color=color, bold=_BOLD, reset=_RESET, dim=dim,
            levelname=record.levelname,
            asctime=record.asctime,
            name=record.name,
            message=record.message,
        )

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class _JsonFormatter(logging.Formatter):
    """
    JSON-форматтер для структурированного логирования.
    Совместим с ELK Stack / Grafana Loki.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
            "module":  record.module,
            "func":    record.funcName,
            "line":    record.lineno,
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class _PlainFormatter(logging.Formatter):
    """Простой текстовый форматтер для файла."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


# ──────────────────────────────────────────────────────────────
# Инициализация
# ──────────────────────────────────────────────────────────────
_initialized = False


def _setup() -> None:
    """Однократная инициализация корневого логгера."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)

    # ── Консольный хендлер ────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(numeric_level)
    # JSON в консоль если явно задан LOG_JSON=true
    console.setFormatter(_JsonFormatter() if LOG_JSON else _ColorFormatter())
    root.addHandler(console)

    # ── Файловый хендлер с ротацией ───────────────────────────
    log_path = Path(LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    # Файл всегда в JSON для удобства парсинга
    file_handler.setFormatter(_JsonFormatter())
    root.addHandler(file_handler)

    # Подавить шум от сторонних библиотек
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    root.info(
        "Логирование инициализировано: level=%s file=%s json=%s",
        LOG_LEVEL, LOG_FILE, LOG_JSON,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Получить логгер для модуля.

    Пример:
        logger = get_logger(__name__)
        logger.info("Сообщение")
    """
    _setup()
    return logging.getLogger(name)
