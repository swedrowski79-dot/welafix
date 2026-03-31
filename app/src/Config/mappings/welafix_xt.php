<?php
/**
 * welafix_xt.php
 *
 * Codex-Implementationsziel:
 * - Diese Datei ist die Mapping-Quelle für die Welafix → XT-Commerce Befüllung.
 * - Codex soll Code schreiben, der diese Expressions auswertet und die Zieltabellen befüllt (Insert/Upsert).
 *
 * Expression-Grammatik (muss Codex implementieren):
 * - "auto"                       => Zielspalte wird per Auto-ID/Autoincrement/Sequenz gesetzt (Importer liefert neue ID zurück)
 * - "default:<value>"            => Konstante (int/string/bool). Beispiele: default:1, default:"", default:de
 * - "<entity>.<field>"           => Feld aus Input-Datensatz (z.B. artikel.Bezeichnung, warengruppe.Beschreibung)
 * - "<expr1> oder <expr2>"       => COALESCE/Fallback (erst expr1, sonst expr2)
 * - "md5(<expr>)"                => md5() über ausgewerteten String
 * - "<expr>+<number>"            => einfache Addition (int)
 * - "filename"                   => Dateiname aus Media-Quelle ableiten (Codex muss definieren, woher)
 * - "calc:<name>"                => Berechnete Werte durch Code (Custom Resolver)
 *
 * calc-Resolver (muss Codex implementieren):
 * - calc:now                         => aktuelle Zeit (z.B. Y-m-d H:i:s)
 * - calc:nested_set_left              => categories_left per Nested-Set Rebuild aus parent_id/sort_order
 * - calc:nested_set_right             => categories_right per Nested-Set Rebuild aus parent_id/sort_order
 * - calc:seo_link_type                => link_type abhängig vom Datensatz (product/category)
 */

return [
  'version' => 1,
  'name'    => 'welafix_xt',

  // Welche Entities der Importer erwarten darf (Input)
  'entities' => [
    'artikel'     => ['key' => 'afs_artikel_id'],
    'warengruppe' => ['key' => 'afs_id'],
    'media'       => ['key' => 'id'],
  ],

  'targets' => [

    // =========================
    // XT: Kategorien
    // =========================
    'xt_categories' => [
      'table' => 'xt_categories',
      'base' => 'warengruppe',
      'primary_key' => 'categories_id',
      'unique_key' => ['external_id'],
      'mode' => 'upsert',
      'columns' => [
        'categories_id'              => 'auto',
        'external_id'                => 'warengruppe.afs_wg_id',
        'permission_id'              => 'default:""',
        'categories_owner'           => 'default:1',
        'categories_image'           => 'warengruppe.Bild oder lookup(xt_categories, external_id, warengruppe.afs_wg_id, categories_image)',
        'categories_left'            => 'calc:nested_set_left',
        'categories_right'           => 'calc:nested_set_right',
        'categories_level'           => 'warengruppe.Ebene+1',
        'parent_id'                  => 'lookup(xt_categories, external_id, warengruppe.parent_id, categories_id)',
        'categories_status'          => 'default:1',
        'categories_template'        => 'default:""',
        'listing_template'           => 'default:""',
        'sort_order'                 => 'default:0',
        'products_sorting'           => 'default:""',
        'products_sorting2'          => 'default:""',
        'top_category'               => 'default:0',
        'start_page_category'        => 'default:0',
        'date_added'                 => 'calc:now',
        'last_modified'              => 'calc:now',
        'category_custom_link'       => 'default:0',
        'category_custom_link_type'  => 'default:""',
        'category_custom_link_id'    => 'default:0',
        'google_product_cat'         => 'default:0',
        'categories_master_image'    => 'warengruppe.Bild_gross oder lookup(xt_categories, external_id, warengruppe.afs_wg_id, categories_master_image)',
      ],
    ],

    'xt_categories_description' => [
      'table' => 'xt_categories_description',
      'base' => 'warengruppe',
      'primary_key' => ['categories_id', 'language_code'],
      'mode' => 'upsert',
      'columns' => [
        'categories_id'              => 'lookup(xt_categories, external_id, warengruppe.afs_wg_id, categories_id)',
        'language_code'              => 'default:de',
        'categories_name'            => 'warengruppe.name',
        'categories_heading_title'   => 'warengruppe.name',
        'categories_description'     => 'warengruppe.Beschreibung',
        'categories_description_bottom'=> 'default:""', 
        'categories_store_id'=> 'default:"1"',

      ],
    ],

    // =========================
    // XT: Media (für Kategorien/Produkte)
    // =========================
    'xt_media' => [
      'table' => 'xt_media',
      'base' => 'media',
      'primary_key' => 'id',
      'unique_key' => ['external_id'],
      'mode' => 'upsert',
      'columns' => [
        'id'                => 'auto',
        'file'              => 'filename',
        'type'              => 'media.type',
        'class'             => 'media.source',
        'download_status'   => 'default:free',
        'status'            => 'calc:media_status',
        'owner'             => 'default:1',
        'date_added'           => 'calc:now',
        'last_modified'     => 'calc:now',
        'max_dl_count'         => 'default:"0"',
        'max_dl_days'         => 'default:"0"',
        'total_downloads'         => 'default:0',
        'copyright_holder'            => 'default:""',
        'external_id'             => 'media.id',
      ],
    ],

    'xt_media_description' => [
      'table' => 'xt_media_description',
      'base' => 'media',
      'primary_key' => ['id', 'language_code'],
      'mode' => 'upsert',
      'columns' => [
        'id'              => 'lookup(xt_media, external_id, media.id, id)',
        'language_code'   => 'default:de',
        'media_name'      => 'default:""',
        'media_description'=> 'default:""',
      ],
    ],

    'xt_media_file_types' => [
      'table' => 'xt_media_file_types',
      'primary_key' => 'mft_id',
      'mode' => 'upsert',
      'columns' => [
        'mft_id'    => 'auto',
        'file_ext'  => 'default:""',
        'file_type' => 'default:""',
      ],
    ],

    'xt_media_gallery' => [
      'table' => 'xt_media_gallery',
      'primary_key' => 'mg_id',
      'mode' => 'upsert',
      'columns' => [
        'mg_id'       => 'auto',
        'class'       => 'default:""',
        'type'        => 'default:""',
        'name'        => 'default:""',
        'status'      => 'default:1',
        'owner'       => 'default:1',
      ],
    ],

    // =========================
    // XT: Produktattribute
    // =========================
    'xt_plg_products_attributes' => [
      'table' => 'xt_plg_products_attributes',
      'base' => 'attributes',
      'primary_key' => 'attributes_id',
      'mode' => 'upsert',
      'columns' => [
        'attributes_id'           => 'attributes.attributes_id',
        'attributes_parent'       => 'attributes.attributes_parent',
        'attributes_model'        => 'attributes.attributes_model',
        'attributes_image'        => 'attributes.attributes_image',
        'attributes_color'        => 'attributes.attributes_color',
        'sort_order'              => 'attributes.sort_order',
        'status'                  => 'attributes.status',
        'attributes_templates_id' => 'attributes.attributes_templates_id',
        'bw_id'                   => 'attributes.bw_id',
      ],
    ],

    'xt_plg_products_attributes_description' => [
      'table' => 'xt_plg_products_attributes_description',
      'base' => 'attributes',
      'primary_key' => ['attributes_id', 'language_code'],
      'mode' => 'upsert',
      'columns' => [
        'attributes_id'   => 'attributes.attributes_id',
        'language_code'   => 'default:de',
        'attributes_name' => 'attributes.attributes_model',
        'attributes_desc' => 'default:""',
      ],
    ],

   /* 'xt_media_to_media_gallery' => [
      'table' => 'xt_media_to_media_gallery',
      'primary_key' => 'ml_id',
      'mode' => 'upsert',
      'columns' => [
        'ml_id' => 'xt_media_link.ml_id',
        'm_id'  => 'xt_media.id',
        'mg_id' => 'xt_media.mg_id',
      ],
    ],
*/
    // =========================
    // XT: Produkte
    // =========================
    'xt_products' => [
      'table' => 'xt_products',
      'base' => 'artikel',
      'primary_key' => 'products_id',
      'unique_key' => ['external_id'],
      'mode' => 'upsert',
      'columns' => [
        'products_id'                     => 'auto',
        'external_id'                     => 'artikel.afs_artikel_id',
        'permission_id'                   => 'default:0',
        'products_owner'                  => 'default:0',
        'products_ean'                    => 'default:""',           // falls du EAN willst: artikel.EANNummer
        'products_quantity'               => 'artikel.Bestand',
        'show_stock'                      => 'default:0',
        'products_average_quantity'       => 'default:0',
        'products_shippingtime'          => 'artikel.ZusatzFeld01',
        'products_shippingtime_nostock'  => 'default:""',
        'products_model'                  => 'artikel.artikelnummer',
        'products_master_flag'            => 'artikel.is_master oder default:0',
        'products_master_model'           => 'artikel.master_modell',

        // Excel hatte "default.2" → normalisiert zu default:2
        'ms_open_first_slave'             => 'default:2',
        'ms_show_slave_list'              => 'default:2',
        'ms_filter_slave_list'            => 'default:2',
        'ms_filter_slave_list_hide_on_product' => 'default:2',
        'products_image_from_master'      => 'default:2',
        'ms_load_masters_free_downloads'  => 'default:2',
        'ms_load_masters_main_img'        => 'default:2',

        // ... (deine Excel enthält sehr viele Standardfelder)
        // Ich übernehme sie 1:1 aus deiner Tabelle:
        'products_price'                  => 'round(artikel.VK3,4)',
        'products_weight'                 => 'default:0',
        'products_status'                 => 'calc:artikel_status',
        'products_tax_class_id'           => 'default:0',
        'date_added'                      => 'calc:now',
        'last_modified'                   => 'calc:now',
        'products_unit'                   => 'default:0',
        'products_average_rating'         => 'default:0',
        'products_rating_count'           => 'default:0',
        'products_digital'                => 'default:0',
        'flag_has_specials'               => 'default:0',
        'products_serials'                => 'default:0',
        'total_downloads'                 => 'default:0',
        'group_discount_allowed'          => 'default:1',
        'google_product_cat'              => 'default:""',
        'products_canonical_master'       => 'default:0',
      ],
    ],

    'xt_products_description' => [
      'table' => 'xt_products_description',
      'base' => 'artikel',
      'primary_key' => ['products_id', 'language_code'],
      'mode' => 'upsert',
      'columns' => [
        'products_id'           => 'lookup(xt_products, external_id, artikel.afs_artikel_id, products_id)',
        'language_code'         => 'default:de',
        'reload_st'             => 'default:0',
        'products_name'         => 'artikel.Bezeichnung',
        'products_description'  => 'artikel.Langtext',
        'products_short_description' => 'default:""',
        'products_keywords'     => 'default:""',
        'products_url'   => 'default:""',
        'products_store_id' => 'default:1',
      ],
    ],

    'xt_plg_products_to_attributes' => [
      'table' => 'xt_plg_products_to_attributes',
      'base' => 'artikel_attribute_map',
      'primary_key' => ['products_id', 'attributes_id', 'attributes_parent_id'],
      'mode' => 'upsert',
      'columns' => [
        'products_id'          => 'lookup(xt_products, external_id, artikel_attribute_map.afs_artikel_id, products_id)',
        'attributes_id'        => 'lookup(xt_plg_products_attributes, attributes_id, artikel_attribute_map.attributes_id, attributes_id)',
        'attributes_parent_id' => 'lookup(xt_plg_products_attributes, attributes_id, artikel_attribute_map.attributes_parent_id, attributes_id)',
      ],
    ],

    'xt_media_link' => [
      'table' => 'xt_media_link',
      'base' => 'artikel_media_map',
      'primary_key' => 'ml_id',
      'unique_key' => ['m_id', 'link_id'],
      'mode' => 'upsert',
      'columns' => [
        'ml_id'     => 'auto',
        'm_id'      => 'lookup(xt_media, external_id, artikel_media_map.media_id, id)',
        'link_id'   => 'lookup(xt_products, external_id, artikel_media_map.afs_artikel_id, products_id)',
        'class'     => 'default:product',
        'type'      => 'default:images',
        'sort_order'=> 'artikel_media_map.position',
      ],
    ],

    // =========================
    // XT: Produkt ↔ Kategorie Zuordnung
    // =========================
    'xt_products_to_categories' => [
      'table' => 'xt_products_to_categories',
      'base' => 'artikel',
      'primary_key' => ['products_id', 'categories_id'],
      'mode' => 'upsert',
      'columns' => [
        'products_id'   => 'lookup(xt_products, external_id, artikel.afs_artikel_id, products_id)',
        'categories_id' => 'lookup(xt_categories, external_id, artikel.warengruppe_id, categories_id)',
        'master_link'   => 'default:1',
        'store_id'      => 'default:1',
      ],
    ],

    // =========================
    // XT: SEO URL
    // =========================
    'xt_seo_url_products' => [
      'table' => 'xt_seo_url',
      'base' => 'artikel',
      'unique_key' => ['url_md5'],
      'primary_key' => ['link_id', 'link_type', 'language_code', 'store_id'],
      'mode' => 'upsert',
      'columns' => [
        'url_md5'          => 'default:md5(artikel.seo_url)',
        'url_text'         => 'default:artikel.seo_url',
        'language_code'    => 'default:de',
        'link_type'        => '1',
        'link_id'          => 'lookup(xt_products, external_id, artikel.afs_artikel_id, products_id)',
        'meta_title'       => 'artikel.meta_title',
        'meta_description' => 'artikel.meta_description',
        'meta_keywords'    => 'default:""',
        'store_id'         => 'default:1',
      ],
    ],

    'xt_seo_url_wg' => [
      'table' => 'xt_seo_url',
      'base' => 'warengruppe',
      'unique_key' => ['url_md5'],
      'primary_key' => ['link_id', 'link_type', 'language_code', 'store_id'],
      'mode' => 'upsert',
      'columns' => [
        'url_md5'          => 'default:md5(warengruppe.seo_url)',
        'url_text'         => 'default:warengruppe.seo_url',
        'language_code'    => 'default:de',
        'link_type'        => '2',
        'link_id'          => 'lookup(xt_categories, external_id, warengruppe.afs_wg_id, categories_id)',
        'meta_title'       => 'warengruppe.meta_title',
        'meta_description' => 'warengruppe.meta_description',
        'meta_keywords'    => 'default:""',
        'store_id'         => 'default:1',
      ],
    ],

  ],
];
