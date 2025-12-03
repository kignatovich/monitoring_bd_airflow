#!/usr/bin/env python3
import os
import socket
import time
from contextlib import closing

import psycopg2
from psycopg2 import OperationalError
from prometheus_client import start_http_server, Gauge


def env(name, default=None, required=False, cast=str):
    val = os.getenv(name, default)
    if required and val is None:
        raise RuntimeError(f"Env var {name} is required")
    if val is None:
        return None
    try:
        return cast(val)
    except Exception:
        return default


# ---------- Настройки из ENV ----------

PG_HOST = env("AF_PG_HOST", "127.0.0.1")
PG_PORT = env("AF_PG_PORT", "5432", cast=int)
PG_DB = env("AF_PG_DB", "airflow")
PG_USER = env("AF_PG_USER", "airflow")
PG_PASSWORD = env("AF_PG_PASSWORD", required=True)

SCRAPE_INTERVAL = env("AF_SCRAPE_INTERVAL_SECONDS", 10, cast=int)

EXPORTER_PORT = env("AF_EXPORTER_PORT", 9105, cast=int)
EXPORTER_ADDR = env("AF_EXPORTER_ADDR", "0.0.0.0")

# Пороги (для airflow_pg_status, 0=OK,1=WARNING,2=CRITICAL)
WARN_IDLE_IN_TX = env("AF_WARN_IDLE_IN_TX", 5, cast=int)
WARN_ACTIVE_CONN = env("AF_WARN_ACTIVE_CONN", 80, cast=int)
WARN_LATENCY_MS = env("AF_WARN_LATENCY_MS", 500, cast=int)


# ---------- Prometheus метрики ----------

# 1 если всё ок (подключились и SELECT 1 прошёл), иначе 0
PG_UP = Gauge("airflow_pg_up", "Airflow Postgres availability (1=up,0=down)")

# Время ответа SELECT 1, миллисекунды (если ошибка, выставляем -1)
PG_LATENCY_MS = Gauge("airflow_pg_latency_ms", "Airflow Postgres SELECT 1 latency in ms")

# Общее кол-во коннектов к БД (по datname)
PG_DB_TOTAL = Gauge("airflow_pg_db_connections_total", "Total connections to Airflow DB")
PG_DB_ACTIVE = Gauge("airflow_pg_db_connections_active", "Active connections to Airflow DB")
PG_DB_IDLE_IN_TX = Gauge(
    "airflow_pg_db_connections_idle_in_tx", "Idle in transaction connections to Airflow DB"
)

# Конкретно коннекты от Airflow/Celery по application_name
PG_AF_TOTAL = Gauge(
    "airflow_pg_af_connections_total", "Total Airflow/Celery connections to Airflow DB"
)
PG_AF_ACTIVE = Gauge(
    "airflow_pg_af_connections_active", "Active Airflow/Celery connections to Airflow DB"
)
PG_AF_IDLE_IN_TX = Gauge(
    "airflow_pg_af_connections_idle_in_tx",
    "Idle in transaction Airflow/Celery connections to Airflow DB",
)

# Интегральный статус: 0=OK, 1=WARNING, 2=CRITICAL
PG_STATUS = Gauge(
    "airflow_pg_status",
    "Airflow Postgres health status (0=OK,1=WARNING,2=CRITICAL)",
)


def tcp_check(host: str, port: int, timeout: float = 3.0) -> bool:
    """Проверяем, что до Postgres можно достучаться по TCP."""
    try:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))
        return True
    except OSError:
        return False


def scrape_once():
    """
    Один цикл опроса Postgres:
    - TCP check
    - SELECT 1
    - pg_stat_activity
    - обновление метрик.
    """
    status = 0  # 0=OK,1=WARNING,2=CRITICAL

    # ---------- TCP check ----------
    if not tcp_check(PG_HOST, PG_PORT):
        PG_UP.set(0)
        PG_LATENCY_MS.set(-1)
        PG_STATUS.set(2)
        # Остальные gauge оставим как есть (последние значения)
        return

    # ---------- Подключение к Postgres ----------
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=3,
            application_name="airflow_pg_exporter",
        )
        conn.autocommit = True
    except OperationalError:
        PG_UP.set(0)
        PG_LATENCY_MS.set(-1)
        PG_STATUS.set(2)
        return

    PG_UP.set(1)

    latency_ms = -1
    db_total = db_active = db_idle_in_tx = 0
    af_total = af_active = af_idle_in_tx = 0

    try:
        with conn.cursor() as cur:
            # ---------- SELECT 1 ----------
            start = time.time()
            cur.execute("SELECT 1;")
            row = cur.fetchone()
            latency_ms = (time.time() - start) * 1000

            if row != (1,):
                status = max(status, 2)

            # ---------- Общая стата по сессиям ----------
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN state = 'idle in transaction' THEN 1 ELSE 0 END) AS idle_in_tx,
                    COUNT(*) AS total
                FROM pg_stat_activity
                WHERE datname = %s;
                """,
                (PG_DB,),
            )
            db_active, db_idle_in_tx, db_total = cur.fetchone() or (0, 0, 0)

            # ---------- Только Airflow/Celery ----------
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN state = 'idle in transaction' THEN 1 ELSE 0 END) AS idle_in_tx,
                    COUNT(*) AS total
                FROM pg_stat_activity
                WHERE datname = %s
                  AND (application_name ILIKE 'airflow%%'
                       OR application_name ILIKE 'celery%%');
                """,
                (PG_DB,),
            )
            af_active, af_idle_in_tx, af_total = cur.fetchone() or (0, 0, 0)

    except Exception:
        status = max(status, 2)
    finally:
        conn.close()

    # ---------- Анализ порогов ----------
    if latency_ms >= 0:
        PG_LATENCY_MS.set(latency_ms)
        if latency_ms > WARN_LATENCY_MS:
            status = max(status, 1)
    else:
        PG_LATENCY_MS.set(-1)
        status = max(status, 2)

    PG_DB_TOTAL.set(db_total)
    PG_DB_ACTIVE.set(db_active)
    PG_DB_IDLE_IN_TX.set(db_idle_in_tx)

    PG_AF_TOTAL.set(af_total)
    PG_AF_ACTIVE.set(af_active)
    PG_AF_IDLE_IN_TX.set(af_idle_in_tx)

    if db_idle_in_tx > WARN_IDLE_IN_TX:
        status = max(status, 1)
    if db_active > WARN_ACTIVE_CONN:
        status = max(status, 1)

    PG_STATUS.set(status)


def main():
    # Стартуем HTTP-сервер для /metrics
    start_http_server(EXPORTER_PORT, addr=EXPORTER_ADDR)
    print(
        f"Starting Airflow↔Postgres exporter on {EXPORTER_ADDR}:{EXPORTER_PORT}, "
        f"scrape interval {SCRAPE_INTERVAL}s"
    )
    while True:
        scrape_once()
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
