Sprache: Deutsch.

Architektur: Schnittstelle liest nur MSSQL (AFS) + schreibt SQLite. Kein MySQL in der Schnittstelle.

Konfiguration über app/src/Config/mappings/*.php.

Schritt 1 zuerst umsetzen: Artikel + Warengruppen importieren, Change-Tracking (changed, last_seen_at, change_reason) und Offline-Erkennung bei Artikeln.

Warengruppen: ParentID steht im Feld Anhang; daraus wird lokal (in PHP) ein path berechnet und in SQLite gespeichert.

Keine Frameworks einführen (plain PHP).

Logging nach logs/app.log.

Jede Änderung klein halten, klar strukturierte Klassen in app/src/....
