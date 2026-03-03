<?php

return [
  'source' => [
    'db' => 'xt_api',
    'table' => 'products',
    'where' => '1=1',
    'key' => 'products_id',
  ],
  'select' => [
    'products_id',
    'products_model',
    'products_status',
    'products_quantity',
  ],
];
