<?php
/**
 * XT-Commerce → SQLite Full Table Pull (1:1)
 *
 * Ziel: Ganze Tabellen aus XT-Commerce holen und 1:1 in SQLite spiegeln.
 * Anforderungen:
 * - Alle Spalten übernehmen (SELECT *)
 * - Zieltabellen heißen gleich wie Quelltabellen
 * - Batchweise kopieren (Batch size kommt aus Dashboard: 500/1000/2000/5000/10000)
 *
 * Codex MUSS implementieren:
 * 1) Schema-Introspektion auf Source (XT):
 *    - Spaltenliste, Typen, Nullability, Default (mindestens Spaltennamen sind Pflicht)
 *    - Primary Key / Unique Key erkennen (wenn vorhanden)
 *
 * 2) Target-Schema in SQLite:
 *    - Wenn Ziel-Tabelle nicht existiert: anlegen (mindestens alle Spalten als TEXT/NUMERIC kompatibel)
 *    - Wenn existiert: fehlende Spalten per ALTER TABLE ADD COLUMN hinzufügen
 *    - NICHT versuchen, Spaltentypen nachträglich zu ändern (SQLite eingeschränkt)
 *
 * 3) Datenkopie:
 *    - Lesen im Cursor/Streaming (kein fetchAll)
 *    - Batch INSERT/UPSERT:
 *      - Wenn PK/Unique-Key vorhanden: UPSERT (INSERT ... ON CONFLICT DO UPDATE)
 *      - Wenn kein Key: Tabelle vorher leeren und dann INSERT (truncate strategy) ODER append (konfigurierbar)
 *
 * 4) Memory-Sicherheit:
 *    - pro Batch verarbeiten, dann unset + gc_collect_cycles()
 */

return [
  'name' => 'xt_commerce_full_tables',
  'version' => 1,

  'defaults' => [
    'source_db' => 'xt',      // Connection-Key in deinem Projekt für XT-Commerce
    'target_db' => 'sqlite',  // lokale DB
    'batch_sizes' => [500, 1000, 2000, 5000, 10000],
    'select' => '*',

    // Wenn kein PK/Unique Key erkannt wird:
    // - 'truncate_insert' = Ziel leeren und komplett neu befüllen
    // - 'append'          = nur anhängen (nicht empfohlen)
    'no_key_strategy' => 'truncate_insert',
    // Für Full-Table Sync: Zieltabellen vorher leeren
    'truncate_before_import' => true,
  ],

  'jobs' => [
    [
      'id' => 'pull_xt_categories',
      'source' => ['table' => 'xt_categories'],
      'target' => ['table' => 'xt_categories'],
    ],
    [
      'id' => 'pull_xt_categories_description',
      'source' => ['table' => 'xt_categories_description'],
      'target' => ['table' => 'xt_categories_description'],
    ],
    [
      'id' => 'pull_xt_media',
      'source' => ['table' => 'xt_media'],
      'target' => ['table' => 'xt_media'],
    ],
    [
      'id' => 'pull_xt_media_description',
      'source' => ['table' => 'xt_media_description'],
      'target' => ['table' => 'xt_media_description'],
    ],
    [
      'id' => 'pull_xt_media_file_types',
      'source' => ['table' => 'xt_media_file_types'],
      'target' => ['table' => 'xt_media_file_types'],
    ],
    [
      'id' => 'pull_xt_media_gallery',
      'source' => ['table' => 'xt_media_gallery'],
      'target' => ['table' => 'xt_media_gallery'],
    ],
    [
      'id' => 'pull_xt_plg_products_attributes_description',
      'source' => ['table' => 'xt_plg_products_attributes_description'],
      'target' => ['table' => 'xt_plg_products_attributes_description'],
    ],
    [
      'id' => 'pull_xt_plg_products_attributes',
      'source' => ['table' => 'xt_plg_products_attributes'],
      'target' => ['table' => 'xt_plg_products_attributes'],
    ],
    [
      'id' => 'pull_xt_media_link',
      'source' => ['table' => 'xt_media_link'],
      'target' => ['table' => 'xt_media_link'],
    ],
    [
      'id' => 'pull_xt_media_to_media_gallery',
      'source' => ['table' => 'xt_media_to_media_gallery'],
      'target' => ['table' => 'xt_media_to_media_gallery'],
    ],
    [
      'id' => 'pull_xt_products',
      'source' => ['table' => 'xt_products'],
      'target' => ['table' => 'xt_products'],
    ],
    [
      'id' => 'pull_xt_products_description',
      'source' => ['table' => 'xt_products_description'],
      'target' => ['table' => 'xt_products_description'],
    ],
    [
      // in deiner Nachricht stand "products_to categories" → korrekt ist:
      'id' => 'pull_xt_products_to_categoriess',
      'source' => ['table' => 'xt_products_to_categories'],
      'target' => ['table' => 'xt_products_to_categories'],
    ],
    [
      'id' => 'pull_xt_seo_url',
      'source' => ['table' => 'xt_seo_url'],
      'target' => ['table' => 'xt_seo_url'],
    ],
  ],
];
