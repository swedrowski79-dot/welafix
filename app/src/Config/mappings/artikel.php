<?php

return [
  'source' => [
    // MSSQL Tabelle (bei dir meist dbo.Artikel)
    'db'    => 'mssql',
    'table' => 'dbo.Artikel',

    // Default-Filter aus dem YAML
    // Mandant = 1
    // Art < 255
    // Artikelnummer IS NOT NULL
    // Internet = 1   (nur Online/Webshop)
    'where' => "Mandant = 1 AND Art < 255 AND Artikelnummer IS NOT NULL AND Internet = 1",

    // Business-Key laut YAML
    'key'   => 'Artikel',
  ],

  // Felderliste wie in deinem YAML (Reihenfolge egal)
  'select' => [
    'Artikel',
    'Art',
    'Artikelnummer',
    'Bezeichnung',
    'EANNummer',
    'Bestand',

    // Bilder 1..10
    'Bild1',
    'Bild2',
    'Bild3',
    'Bild4',
    'Bild5',
    'Bild6',
    'Bild7',
    'Bild8',
    'Bild9',
    'Bild10',

    'VK3',              // Verkaufspreis
    'Warengruppe',
    'Umsatzsteuer',

    'Zusatzfeld01',     // Mindestmenge
    'Zusatzfeld03',     // Attribname1
    'Zusatzfeld04',     // Attribname2
    'Zusatzfeld05',     // Attribname3
    'Zusatzfeld06',     // Attribname4
    'Zusatzfeld15',     // Attribvalue1
    'Zusatzfeld16',     // Attribvalue2
    'Zusatzfeld17',     // Attribvalue3
    'Zusatzfeld18',     // Attribvalue4
    'Zusatzfeld07',     // Einzelartikel/Master/Masterartikelnummer

    'Bruttogewicht',
    'Internet',         // Webshop ja/nein
    'Einheit',
    'Langtext',
    'Werbetext1',
    'Bemerkung',
    'Hinweis',
    'Update',
  ],

  // Optional: nur als Doku/Hint (dein Importer kann das später nutzen)
  'hints' => [
    'fanout_images' => [
      'pattern' => 'Bild{n}',
      'range'   => ['from' => 1, 'to' => 10],
    ],
  ],
];