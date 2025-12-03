# monitoring_bd_airflow


Запуск

```python
sudo dnf install -y python3-pip
sudo pip3 install psycopg2-binary prometheus_client
```

```python
sudo mkdir -p /opt/airflow-health
sudo vi /opt/airflow-health/airflow_pg_healthcheck.py
sudo chmod +x /opt/airflow-health/airflow_pg_healthcheck.py
```

```python
sudo nano /etc/sysconfig/airflow_pg_health
```
```text
AF_PG_HOST=127.0.0.1
AF_PG_PORT=5524           # порт Postgres
AF_PG_DB=airflow
AF_PG_USER=airflow
AF_PG_PASSWORD='123456789'

# Пороги — можно настроить
AF_WARN_IDLE_IN_TX=5
AF_WARN_ACTIVE_CONN=80
AF_WARN_LATENCY_MS=500
```
Создаем службу для запуска service unit

```text
nano /etc/systemd/system/airflow-pg-health.service
```

```text
[Unit]
Description=Airflow ↔ Postgres healthcheck

[Service]
Type=oneshot
EnvironmentFile=/etc/sysconfig/airflow_pg_health
ExecStart=/opt/airflow-health/airflow_pg_healthcheck.py
StandardOutput=journal
StandardError=journal
```

создаем timer unit

```text
/etc/systemd/system/airflow-pg-health.timer
```

```text
[Unit]
Description=Run Airflow ↔ Postgres healthcheck every 5 minutes

[Timer]
OnBootSec=1min
OnUnitActiveSec=5min
Unit=airflow-pg-health.service

[Install]
WantedBy=timers.target
```

активируем 

```text
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-pg-health.timer
sudo systemctl list-timers | grep airflow-pg-health
```

```bash
journalctl -u airflow-pg-health.service --no-pager -n 50
```

Проверка метрик
```bash
curl http://localhost:9105/metrics | head
```

В prometheus.yml добавляем scrape_config
```yaml
scrape_configs:
  - job_name: 'airflow_pg_exporter'
    static_configs:
      - targets:
          - 'HOSTNAME_ГДЕ_БЕЖИТ_ЭКСПОРТЕР:9105'
        labels:
          env: 'prod'
          app: 'airflow'
```
