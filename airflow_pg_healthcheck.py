#!/usr/bin/env python3
import os
import socket
import sys
import time
from contextlib import closing

import psycopg2
from psycopg2 import OperationalError


def env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and not value:
        print(f"CRITICAL: env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return value


def tcp_check(host: str, port: int, timeout: float = 3.0) -> bool:
    """Проверяем, что до Postgres можно достучаться по TCP."""
    try:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))
        return True
    except OSError as e:
        print(f"CRITICAL: TCP connect to {host}:{port} failed: {e}", file=sys.stderr)
        return False


def main():
    # ---------- 1. Параметры подключения ----------
    pg_host = env("AF_PG_HOST", required=True)
    pg_port = int(env("AF_PG_PORT", "5432"))
    pg_db = env("AF_PG_DB", "airflow")
    pg_user = env("AF_PG_USER", "airflow")
    pg_password = env("AF_PG_PASSWORD", required=True)

    # Пороговые значения (можно менять через env)
    warn_idle_in_tx = int(env("AF_WARN_IDLE_IN_TX", "5"))
    warn_active = int(env("AF_WARN_ACTIVE_CONN", "80"))
    warn_latency_ms = int(env("AF_WARN_LATENCY_MS", "500"))

    # ---------- 2. TCP healthcheck ----------
    if not tcp_check(pg_host, pg_port):
        sys.exit(2)

    # ---------- 3. Подключение к Postgres ----------
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            connect_timeout=3,
            application_name="airflow_pg_healthcheck",
        )
        conn.autocommit = True
    except OperationalError as e:
        print(f"CRITICAL: cannot connect to Postgres: {e}", file=sys.stderr)
        sys.exit(2)

    status = 0  # 0 = OK, 1 = WARNING, 2 = CRITICAL
    messages = []

    try:
        with conn.cursor() as cur:
            # ---------- 4. Лёгкий тестовый запрос ----------
            start = time.time()
            cur.execute("SELECT 1;")
            row = cur.fetchone()
            latency_ms = (time.time() - start) * 1000

            if row != (1,):
                messages.append("CRITICAL: SELECT 1 returned unexpected result")
                status = max(status, 2)

            # ---------- 5. Статистика по сессиям ----------
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN state = 'idle in transaction' THEN 1 ELSE 0 END) AS idle_in_tx,
                    COUNT(*) AS total
                FROM pg_stat_activity
                WHERE datname = %s;
                """,
                (pg_db,),
            )
            active, idle_in_tx, total = cur.fetchone() or (0, 0, 0)

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
                (pg_db,),
            )
            af_active, af_idle_in_tx, af_total = cur.fetchone() or (0, 0, 0)

    except Exception as e:
        print(f"CRITICAL: error during health query: {e}", file=sys.stderr)
        status = max(status, 2)
        latency_ms = -1
        active = idle_in_tx = total = 0
        af_active = af_idle_in_tx = af_total = 0
    finally:
        conn.close()

    # ---------- 6. Анализ ----------
    if latency_ms >= 0 and latency_ms > warn_latency_ms:
        messages.append(
            f"WARNING: SELECT 1 latency is {latency_ms:.0f} ms (> {warn_latency_ms} ms)."
        )
        status = max(status, 1)

    if idle_in_tx > warn_idle_in_tx:
        messages.append(
            f"WARNING: idle in transaction sessions = {idle_in_tx} "
            f"(threshold {warn_idle_in_tx})."
        )
        status = max(status, 1)

    if active > warn_active:
        messages.append(
            f"WARNING: active sessions = {active} (threshold {warn_active})."
        )
        status = max(status, 1)

    # ---------- 7. Итоговый вывод ----------
    label = ["OK", "WARNING", "CRITICAL"][status]
    base_msg = (
        f"PG_HEALTH status={label} | "
        f"latency_ms={latency_ms:.0f}, "
        f"db_total={total}, db_active={active}, db_idle_in_tx={idle_in_tx}, "
        f"af_total={af_total}, af_active={af_active}, af_idle_in_tx={af_idle_in_tx}"
    )

    if messages:
        print(base_msg + " :: " + " ; ".join(messages))
    else:
        print(base_msg)

    sys.exit(status)


if __name__ == "__main__":
    main()
