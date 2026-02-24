<?php

return [
  'source' => [
    'db'    => 'mssql',
    'table' => 'dbo.Dokument',

    // Default-Filter aus dem YAML
    'where' => "Artikel > 0",

    // Business-Key laut YAML
    'key'   => 'Zaehler',
  ],

  'select' => [
    'Zaehler',
    'Artikel',
    'Dateiname',
    'Titel',
    'Art',
  ],
];