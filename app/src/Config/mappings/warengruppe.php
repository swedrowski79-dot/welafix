<?php

return [
  'source' => [
    'db'    => 'mssql',
    'table' => 'dbo.Warengruppe',

    // Default-Filter aus dem YAML
    // Mandant = 1
    // Internet = 0  (laut deiner alten Config)
    'where' => "Mandant = 1 AND Internet = 0",

    // Business-Key laut YAML
    'key'   => 'Warengruppe',
  ],

  'select' => [
    'Warengruppe',
    'Art',
    'Anhang',
    'Ebene',
    'Bezeichnung',
    'Internet',
    'Bild',
    'Bild_gross',
    'Beschreibung',
    
  ],

  // Hinweis aus YAML: on_update_column = Update
  'hints' => [
    'on_update_column' => 'Update',
  ],
];