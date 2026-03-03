<?php

return [
  'source' => [
    'db' => 'xt_api',
    'table' => 'products_description',
    'where' => '1=1',
    'key' => 'products_id',
  ],
  'select' => [
    'products_id',
    'products_name',
    'products_short_description',
    'products_description',
    'language_code',
  ],
];
