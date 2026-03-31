# Legacy XT Import Path

Status: deaktiviert

Betroffener Pfad:
- `/sync/xt`

Warum der Pfad deaktiviert wurde:
- Der produktiv genutzte XT-Weg ist aktuell `xt-mapping` und optional `xt-full`.
- Fuer `/sync/xt` gibt es im Dashboard keinen eigenen Button mehr.
- Der Codepfad verwendet `XtImportService`, das `MappingLoader->load(...)` aufruft. Diese Methode existiert im aktuellen `MappingLoader` nicht mehr, dort gibt es nur `loadAll()`.
- Dadurch ist der Pfad technisch fehleranfaellig und kann bei spaeter Nutzung mit einem Fatal Error scheitern.

Warum der Code nicht geloescht wurde:
- Ein spaeterer Rueckbau soll moeglich bleiben.
- Der alte Service-Code wurde bewusst in diesen Legacy-Ordner verschoben.
- Der aktive Codepfad unter `app/src` bleibt dadurch sauber.

Rueckgaengig machen:
1. Die Datei `legacy/xt-import-path/XtImportService.php` bei Bedarf wieder nach `app/src/Domain/Xt/XtImportService.php` zurueckholen.
2. Die Route `/sync/xt` in `app/src/Bootstrap/App.php` wieder auf `XtImportService->import(...)` umstellen.
3. Vorher `XtImportService` reparieren:
   - entweder `MappingLoader` wieder um eine `load($name)`-Methode erweitern
   - oder `XtImportService` auf `loadAll()` plus gezielte Auswahl des Mappings umbauen
4. Danach den Pfad mit einem echten Testlauf pruefen, bevor er wieder benutzt wird.

Empfohlene aktive XT-Wege:
- `/sync/xt-mapping`
- `/sync/xt-full`

Wenn die Schnittstelle spaeter komplett getestet und der alte Pfad sicher nicht mehr gebraucht wird, kann dieser ganze Ordner geloescht werden.

