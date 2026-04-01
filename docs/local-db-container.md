# Lokale Arbeitsdatenbank im Docker-Setup

Die lokale Arbeitsdatenbank kann jetzt im Projekt per Docker Compose als MariaDB-Testinstanz gestartet werden.

Container:
- Service: `welafix-db`
- Image: `mariadb:11`
- Host-Port: `3307`
- Container-Port: `3306`
- Datenverzeichnis auf dem Host: `./storage/mysql`

Relevante Env-Variablen:
- `LOCAL_DB_DRIVER`
- `LOCAL_DB_HOST`
- `LOCAL_DB_PORT`
- `LOCAL_DB_NAME`
- `LOCAL_DB_USER`
- `LOCAL_DB_PASS`
- `LOCAL_DB_ROOT_PASSWORD`

Beispielstart:

```bash
docker compose up -d --build
```

Direkter Verbindungscheck:

```bash
docker compose exec welafix-db mariadb -uwelafix -pwelafix welafix -e "SELECT VERSION();"
```

Wichtig:
- Die Anwendung nutzt aktuell weiterhin standardmäßig `SQLITE_PATH`.
- Der MariaDB-Container ist die vorbereitete Zielumgebung für die geplante Umstellung der lokalen Arbeitsdatenbank.
- Die Daten liegen absichtlich nicht in einem Docker-Volume, sondern sichtbar im Projekt unter `storage/mysql`.
