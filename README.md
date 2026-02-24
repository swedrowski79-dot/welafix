# Welafix Schnittstelle (Start-Gerüst)

## Start
1) Docker build & run:
   docker compose up -d --build

2) Browser:
   http://localhost:8088

## MSSQL Test
http://localhost:8088/test_mssql.php

## SQLite
Die SQLite DB liegt in:
storage/app.db

Beim ersten Aufruf von / wird die Migration ausgeführt, falls app.db leer ist.
