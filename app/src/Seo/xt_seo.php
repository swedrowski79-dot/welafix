<?php
declare(strict_types=1);

function xt_filterAutoUrlText_inline(string $string, string $languageCode = 'de', string $stringGlue = '-', $class_ = false, $id_ = false): string
{
    $replaceMapAll = [
        'Á'=>'A','Í'=>'I','Ò'=>'O','Ý'=>'Y','á'=>'a','é'=>'e','í'=>'i','ó'=>'o','ý'=>'y',
        'À'=>'A','È'=>'E','Ì'=>'I','Ù'=>'U','à'=>'a','è'=>'e','ì'=>'i','ò'=>'o','ù'=>'u',
        'Â'=>'A','Ê'=>'E','Î'=>'I','Ô'=>'O','Û'=>'U','â'=>'a','ê'=>'e','î'=>'i','ô'=>'o','û'=>'u',
        'Æ'=>'AE','Ç'=>'C','Ð'=>'Eth','Ø'=>'O','Þ'=>'Thorn','æ'=>'ae','ç'=>'c','đ'=>'eth','ø'=>'o','þ'=>'thorn',
        'Ä'=>'ae','Ö'=>'oe','Ü'=>'ue','ä'=>'ae','ü'=>'ue','ö'=>'oe','ß'=>'ss',
    ];

    $replaceSearch = [];
    $replaceReplace = [];
    foreach ($replaceMapAll as $lookup => $repl) {
        $replaceSearch[]  = '/' . $lookup . '/u';
        $replaceReplace[] = $repl;
    }

    $stopWordsDe = [
        'aber','als','am','an','auch','auf','aus','bei','bin','bis','bist','da','dadurch','daher','darum','das','daß','dass',
        'dein','deine','dem','den','der','des','deshalb','dessen','die','dies','dieser','dieses','doch','dort','du','durch',
        'ein','eine','einem','einen','einer','eines','er','es','euer','eure','für','hatte','hatten','hattest','hattet',
        'hier','hinter','ich','ihr','ihre','im','in','ist','ja','jede','jedem','jeden','jeder','jedes','jener','jenes','jetzt',
        'kann','kannst','können','könnt','machen','mein','meine','mit','muß','müssen','mußt','musst','müßt','nach','nachdem',
        'nein','nicht','nun','oder','seid','sein','seine','sich','sie','sind','soll','sollen','sollst','sollt','sonst','soweit',
        'sowie','über','und','unser','unsere','unter','vom','von','vor','wann','warum','was','weiter','weitere','wenn','wer',
        'werde','werden','werdet','weshalb','wie','wieder','wieso','wir','wird','wirst','wo','woher','wohin','zu','zum','zur',
    ];

    $stopWords = ($languageCode === 'de') ? $stopWordsDe : [];

    $string = trim($string);
    $string = preg_replace("/\//", "-", $string);

    $words = preg_split("/[\s,.]+/u", $string);

    if (is_array($words) && count($words) > 1) {
        foreach ($words as $k => $w) {
            $w = trim((string)$w);
            $wl = mb_strtolower($w, 'UTF-8');
            if ($w === '' || in_array($wl, $stopWords, true)) {
                unset($words[$k]);
            } else {
                $words[$k] = $w;
            }
        }
    }

    // merge
    $string = implode($stringGlue, $words ?? []);

    // replace chars via replace list (ALL)
    $string = preg_replace($replaceSearch, $replaceReplace, (string)$string);

    // remove everything which is not a number, letter or - / . _
    $string = preg_replace("/[^a-zA-Z0-9\-\/\.\_]/u", "", (string)$string);

    // kill double --
    $string = preg_replace("/(-){2,}/", "-", (string)$string);

    // remove - at the end
    $string = preg_replace("/-$/", "", (string)$string);

    // empty fallback
    if ($string === '') {
        $string = (($class_) ? $class_ : '') . '-' . (($id_) ? $id_ : '') . '-empty';
    }

    return $string;
}
