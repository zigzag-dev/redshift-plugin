import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent } from '@posthog/plugin-scaffold'
import { Client } from 'pg'

// Obtained from https://github.com/unicode-org/cldr-json/blob/main/cldr-json/cldr-localenames-full/main/en/languages.json
// Have to be placed here, as PostHog App only supports single-file execution
// Modified: convert all keys to lower case, as PostHog Events sometimes grab language code in different casings
const icuLanguageJSON = `
{
  "main": {
    "en": {
      "identity": {
        "version": {
          "_cldrVersion": "41"
        },
        "language": "en"
      },
      "localeDisplayNames": {
        "languages": {
          "aa": "Afar",
          "ab": "Abkhazian",
          "ace": "Achinese",
          "ach": "Acoli",
          "ada": "Adangme",
          "ady": "Adyghe",
          "ae": "Avestan",
          "aeb": "Tunisian Arabic",
          "af": "Afrikaans",
          "afh": "Afrihili",
          "agq": "Aghem",
          "ain": "Ainu",
          "ak": "Akan",
          "akk": "Akkadian",
          "akz": "Alabama",
          "ale": "Aleut",
          "aln": "Gheg Albanian",
          "alt": "Southern Altai",
          "am": "Amharic",
          "an": "Aragonese",
          "ang": "Old English",
          "anp": "Angika",
          "ar": "Arabic",
          "ar-001": "Modern Standard Arabic",
          "arc": "Aramaic",
          "arn": "Mapuche",
          "aro": "Araona",
          "arp": "Arapaho",
          "arq": "Algerian Arabic",
          "ars": "Najdi Arabic",
          "ars-alt-menu": "Arabic, Najdi",
          "arw": "Arawak",
          "ary": "Moroccan Arabic",
          "arz": "Egyptian Arabic",
          "as": "Assamese",
          "asa": "Asu",
          "ase": "American Sign Language",
          "ast": "Asturian",
          "atj": "Atikamekw",
          "av": "Avaric",
          "avk": "Kotava",
          "awa": "Awadhi",
          "ay": "Aymara",
          "az": "Azerbaijani",
          "az-alt-short": "Azeri",
          "ba": "Bashkir",
          "bal": "Baluchi",
          "ban": "Balinese",
          "bar": "Bavarian",
          "bas": "Basaa",
          "bax": "Bamun",
          "bbc": "Batak Toba",
          "bbj": "Ghomala",
          "be": "Belarusian",
          "bej": "Beja",
          "bem": "Bemba",
          "bew": "Betawi",
          "bez": "Bena",
          "bfd": "Bafut",
          "bfq": "Badaga",
          "bg": "Bulgarian",
          "bgn": "Western Balochi",
          "bho": "Bhojpuri",
          "bi": "Bislama",
          "bik": "Bikol",
          "bin": "Bini",
          "bjn": "Banjar",
          "bkm": "Kom",
          "bla": "Siksika",
          "blt": "Tai Dam",
          "bm": "Bambara",
          "bn": "Bangla",
          "bo": "Tibetan",
          "bpy": "Bishnupriya",
          "bqi": "Bakhtiari",
          "br": "Breton",
          "bra": "Braj",
          "brh": "Brahui",
          "brx": "Bodo",
          "bs": "Bosnian",
          "bss": "Akoose",
          "bua": "Buriat",
          "bug": "Buginese",
          "bum": "Bulu",
          "byn": "Blin",
          "byv": "Medumba",
          "ca": "Catalan",
          "cad": "Caddo",
          "car": "Carib",
          "cay": "Cayuga",
          "cch": "Atsam",
          "ccp": "Chakma",
          "ce": "Chechen",
          "ceb": "Cebuano",
          "cgg": "Chiga",
          "ch": "Chamorro",
          "chb": "Chibcha",
          "chg": "Chagatai",
          "chk": "Chuukese",
          "chm": "Mari",
          "chn": "Chinook Jargon",
          "cho": "Choctaw",
          "chp": "Chipewyan",
          "chr": "Cherokee",
          "chy": "Cheyenne",
          "cic": "Chickasaw",
          "ckb": "Central Kurdish",
          "ckb-alt-menu": "Kurdish, Central",
          "ckb-alt-variant": "Kurdish, Sorani",
          "clc": "Chilcotin",
          "co": "Corsican",
          "cop": "Coptic",
          "cps": "Capiznon",
          "cr": "Cree",
          "crg": "Michif",
          "crh": "Crimean Tatar",
          "crj": "Southern East Cree",
          "crk": "Plains Cree",
          "crl": "Northern East Cree",
          "crm": "Moose Cree",
          "crr": "Carolina Algonquian",
          "crs": "Seselwa Creole French",
          "cs": "Czech",
          "csb": "Kashubian",
          "csw": "Swampy Cree",
          "cu": "Church Slavic",
          "cv": "Chuvash",
          "cwd": "Woods Cree",
          "cy": "Welsh",
          "da": "Danish",
          "dak": "Dakota",
          "dar": "Dargwa",
          "dav": "Taita",
          "de": "German",
          "de-at": "Austrian German",
          "de-ch": "Swiss High German",
          "del": "Delaware",
          "den": "Slave",
          "dgr": "Dogrib",
          "din": "Dinka",
          "dje": "Zarma",
          "doi": "Dogri",
          "dsb": "Lower Sorbian",
          "dtp": "Central Dusun",
          "dua": "Duala",
          "dum": "Middle Dutch",
          "dv": "Divehi",
          "dyo": "Jola-Fonyi",
          "dyu": "Dyula",
          "dz": "Dzongkha",
          "dzg": "Dazaga",
          "ebu": "Embu",
          "ee": "Ewe",
          "efi": "Efik",
          "egl": "Emilian",
          "egy": "Ancient Egyptian",
          "eka": "Ekajuk",
          "el": "Greek",
          "elx": "Elamite",
          "en": "English",
          "en-au": "Australian English",
          "en-ca": "Canadian English",
          "en-gb": "British English",
          "en-gb-alt-short": "UK English",
          "en-us": "American English",
          "en-us-alt-short": "US English",
          "enm": "Middle English",
          "eo": "Esperanto",
          "es": "Spanish",
          "es-419": "Latin American Spanish",
          "es-es": "European Spanish",
          "es-mx": "Mexican Spanish",
          "esu": "Central Yupik",
          "et": "Estonian",
          "eu": "Basque",
          "ewo": "Ewondo",
          "ext": "Extremaduran",
          "fa": "Persian",
          "fa-af": "Dari",
          "fan": "Fang",
          "fat": "Fanti",
          "ff": "Fulah",
          "fi": "Finnish",
          "fil": "Filipino",
          "fit": "Tornedalen Finnish",
          "fj": "Fijian",
          "fo": "Faroese",
          "fon": "Fon",
          "fr": "French",
          "fr-ca": "Canadian French",
          "fr-ch": "Swiss French",
          "frc": "Cajun French",
          "frm": "Middle French",
          "fro": "Old French",
          "frp": "Arpitan",
          "frr": "Northern Frisian",
          "frs": "Eastern Frisian",
          "fur": "Friulian",
          "fy": "Western Frisian",
          "ga": "Irish",
          "gaa": "Ga",
          "gag": "Gagauz",
          "gan": "Gan Chinese",
          "gay": "Gayo",
          "gba": "Gbaya",
          "gbz": "Zoroastrian Dari",
          "gd": "Scottish Gaelic",
          "gez": "Geez",
          "gil": "Gilbertese",
          "gl": "Galician",
          "glk": "Gilaki",
          "gmh": "Middle High German",
          "gn": "Guarani",
          "goh": "Old High German",
          "gom": "Goan Konkani",
          "gon": "Gondi",
          "gor": "Gorontalo",
          "got": "Gothic",
          "grb": "Grebo",
          "grc": "Ancient Greek",
          "gsw": "Swiss German",
          "gu": "Gujarati",
          "guc": "Wayuu",
          "gur": "Frafra",
          "guz": "Gusii",
          "gv": "Manx",
          "gwi": "Gwichʼin",
          "ha": "Hausa",
          "hai": "Haida",
          "hak": "Hakka Chinese",
          "haw": "Hawaiian",
          "hax": "Southern Haida",
          "hdn": "Northern Haida",
          "he": "Hebrew",
          "hi": "Hindi",
          "hif": "Fiji Hindi",
          "hil": "Hiligaynon",
          "hit": "Hittite",
          "hmn": "Hmong",
          "hnj": "Hmong Njua",
          "ho": "Hiri Motu",
          "hr": "Croatian",
          "hsb": "Upper Sorbian",
          "hsn": "Xiang Chinese",
          "ht": "Haitian Creole",
          "hu": "Hungarian",
          "hup": "Hupa",
          "hur": "Halkomelem",
          "hy": "Armenian",
          "hz": "Herero",
          "ia": "Interlingua",
          "iba": "Iban",
          "ibb": "Ibibio",
          "id": "Indonesian",
          "ie": "Interlingue",
          "ig": "Igbo",
          "ii": "Sichuan Yi",
          "ik": "Inupiaq",
          "ike": "Eastern Canadian Inuktitut",
          "ikt": "Western Canadian Inuktitut",
          "ilo": "Iloko",
          "inh": "Ingush",
          "io": "Ido",
          "is": "Icelandic",
          "it": "Italian",
          "iu": "Inuktitut",
          "izh": "Ingrian",
          "ja": "Japanese",
          "jam": "Jamaican Creole English",
          "jbo": "Lojban",
          "jgo": "Ngomba",
          "jmc": "Machame",
          "jpr": "Judeo-Persian",
          "jrb": "Judeo-Arabic",
          "jut": "Jutish",
          "jv": "Javanese",
          "ka": "Georgian",
          "kaa": "Kara-Kalpak",
          "kab": "Kabyle",
          "kac": "Kachin",
          "kaj": "Jju",
          "kam": "Kamba",
          "kaw": "Kawi",
          "kbd": "Kabardian",
          "kbl": "Kanembu",
          "kcg": "Tyap",
          "kde": "Makonde",
          "kea": "Kabuverdianu",
          "ken": "Kenyang",
          "kfo": "Koro",
          "kg": "Kongo",
          "kgp": "Kaingang",
          "kha": "Khasi",
          "kho": "Khotanese",
          "khq": "Koyra Chiini",
          "khw": "Khowar",
          "ki": "Kikuyu",
          "kiu": "Kirmanjki",
          "kj": "Kuanyama",
          "kk": "Kazakh",
          "kkj": "Kako",
          "kl": "Kalaallisut",
          "kln": "Kalenjin",
          "km": "Khmer",
          "kmb": "Kimbundu",
          "kn": "Kannada",
          "ko": "Korean",
          "koi": "Komi-Permyak",
          "kok": "Konkani",
          "kos": "Kosraean",
          "kpe": "Kpelle",
          "kr": "Kanuri",
          "krc": "Karachay-Balkar",
          "kri": "Krio",
          "krj": "Kinaray-a",
          "krl": "Karelian",
          "kru": "Kurukh",
          "ks": "Kashmiri",
          "ksb": "Shambala",
          "ksf": "Bafia",
          "ksh": "Colognian",
          "ku": "Kurdish",
          "kum": "Kumyk",
          "kut": "Kutenai",
          "kv": "Komi",
          "kw": "Cornish",
          "kwk": "Kwakʼwala",
          "ky": "Kyrgyz",
          "ky-alt-variant": "Kirghiz",
          "la": "Latin",
          "lad": "Ladino",
          "lag": "Langi",
          "lah": "Lahnda",
          "lam": "Lamba",
          "lb": "Luxembourgish",
          "lez": "Lezghian",
          "lfn": "Lingua Franca Nova",
          "lg": "Ganda",
          "li": "Limburgish",
          "lij": "Ligurian",
          "lil": "Lillooet",
          "liv": "Livonian",
          "lkt": "Lakota",
          "lmo": "Lombard",
          "ln": "Lingala",
          "lo": "Lao",
          "lol": "Mongo",
          "lou": "Louisiana Creole",
          "loz": "Lozi",
          "lrc": "Northern Luri",
          "lt": "Lithuanian",
          "ltg": "Latgalian",
          "lu": "Luba-Katanga",
          "lua": "Luba-Lulua",
          "lui": "Luiseno",
          "lun": "Lunda",
          "luo": "Luo",
          "lus": "Mizo",
          "luy": "Luyia",
          "lv": "Latvian",
          "lzh": "Literary Chinese",
          "lzz": "Laz",
          "mad": "Madurese",
          "maf": "Mafa",
          "mag": "Magahi",
          "mai": "Maithili",
          "mak": "Makasar",
          "man": "Mandingo",
          "mas": "Masai",
          "mde": "Maba",
          "mdf": "Moksha",
          "mdr": "Mandar",
          "men": "Mende",
          "mer": "Meru",
          "mfe": "Morisyen",
          "mg": "Malagasy",
          "mga": "Middle Irish",
          "mgh": "Makhuwa-Meetto",
          "mgo": "Metaʼ",
          "mh": "Marshallese",
          "mi": "Māori",
          "mic": "Mi'kmaq",
          "min": "Minangkabau",
          "mk": "Macedonian",
          "ml": "Malayalam",
          "mn": "Mongolian",
          "mnc": "Manchu",
          "mni": "Manipuri",
          "moe": "Innu-aimun",
          "moh": "Mohawk",
          "mos": "Mossi",
          "mr": "Marathi",
          "mrj": "Western Mari",
          "ms": "Malay",
          "mt": "Maltese",
          "mua": "Mundang",
          "mul": "Multiple languages",
          "mus": "Muscogee",
          "mwl": "Mirandese",
          "mwr": "Marwari",
          "mwv": "Mentawai",
          "my": "Burmese",
          "my-alt-variant": "Myanmar Language",
          "mye": "Myene",
          "myv": "Erzya",
          "mzn": "Mazanderani",
          "na": "Nauru",
          "nan": "Min Nan Chinese",
          "nap": "Neapolitan",
          "naq": "Nama",
          "nb": "Norwegian Bokmål",
          "nd": "North Ndebele",
          "nds": "Low German",
          "nds-nl": "Low Saxon",
          "ne": "Nepali",
          "new": "Newari",
          "ng": "Ndonga",
          "nia": "Nias",
          "niu": "Niuean",
          "njo": "Ao Naga",
          "nl": "Dutch",
          "nl-be": "Flemish",
          "nmg": "Kwasio",
          "nn": "Norwegian Nynorsk",
          "nnh": "Ngiemboon",
          "no": "Norwegian",
          "nog": "Nogai",
          "non": "Old Norse",
          "nov": "Novial",
          "nqo": "N’Ko",
          "nr": "South Ndebele",
          "nso": "Northern Sotho",
          "nus": "Nuer",
          "nv": "Navajo",
          "nwc": "Classical Newari",
          "ny": "Nyanja",
          "nym": "Nyamwezi",
          "nyn": "Nyankole",
          "nyo": "Nyoro",
          "nzi": "Nzima",
          "oc": "Occitan",
          "oj": "Ojibwa",
          "ojb": "Northwestern Ojibwa",
          "ojc": "Central Ojibwa",
          "ojg": "Eastern Ojibwa",
          "ojs": "Oji-Cree",
          "ojw": "Western Ojibwa",
          "oka": "Okanagan",
          "om": "Oromo",
          "or": "Odia",
          "os": "Ossetic",
          "osa": "Osage",
          "ota": "Ottoman Turkish",
          "pa": "Punjabi",
          "pag": "Pangasinan",
          "pal": "Pahlavi",
          "pam": "Pampanga",
          "pap": "Papiamento",
          "pau": "Palauan",
          "pcd": "Picard",
          "pcm": "Nigerian Pidgin",
          "pdc": "Pennsylvania German",
          "pdt": "Plautdietsch",
          "peo": "Old Persian",
          "pfl": "Palatine German",
          "phn": "Phoenician",
          "pi": "Pali",
          "pl": "Polish",
          "pms": "Piedmontese",
          "pnt": "Pontic",
          "pon": "Pohnpeian",
          "pqm": "Malecite",
          "prg": "Prussian",
          "pro": "Old Provençal",
          "ps": "Pashto",
          "ps-alt-variant": "Pushto",
          "pt": "Portuguese",
          "pt-br": "Brazilian Portuguese",
          "pt-pt": "European Portuguese",
          "qu": "Quechua",
          "quc": "Kʼicheʼ",
          "qug": "Chimborazo Highland Quichua",
          "raj": "Rajasthani",
          "rap": "Rapanui",
          "rar": "Rarotongan",
          "rgn": "Romagnol",
          "rhg": "Rohingya",
          "rif": "Riffian",
          "rm": "Romansh",
          "rn": "Rundi",
          "ro": "Romanian",
          "ro-md": "Moldavian",
          "rof": "Rombo",
          "rom": "Romany",
          "rtm": "Rotuman",
          "ru": "Russian",
          "rue": "Rusyn",
          "rug": "Roviana",
          "rup": "Aromanian",
          "rw": "Kinyarwanda",
          "rwk": "Rwa",
          "sa": "Sanskrit",
          "sad": "Sandawe",
          "sah": "Sakha",
          "sam": "Samaritan Aramaic",
          "saq": "Samburu",
          "sas": "Sasak",
          "sat": "Santali",
          "saz": "Saurashtra",
          "sba": "Ngambay",
          "sbp": "Sangu",
          "sc": "Sardinian",
          "scn": "Sicilian",
          "sco": "Scots",
          "sd": "Sindhi",
          "sdc": "Sassarese Sardinian",
          "sdh": "Southern Kurdish",
          "se": "Northern Sami",
          "se-alt-menu": "Sami, Northern",
          "see": "Seneca",
          "seh": "Sena",
          "sei": "Seri",
          "sel": "Selkup",
          "ses": "Koyraboro Senni",
          "sg": "Sango",
          "sga": "Old Irish",
          "sgs": "Samogitian",
          "sh": "Serbo-Croatian",
          "shi": "Tachelhit",
          "shn": "Shan",
          "shu": "Chadian Arabic",
          "si": "Sinhala",
          "sid": "Sidamo",
          "sk": "Slovak",
          "sl": "Slovenian",
          "slh": "Southern Lushootseed",
          "sli": "Lower Silesian",
          "sly": "Selayar",
          "sm": "Samoan",
          "sma": "Southern Sami",
          "sma-alt-menu": "Sami, Southern",
          "smj": "Lule Sami",
          "smj-alt-menu": "Sami, Lule",
          "smn": "Inari Sami",
          "smn-alt-menu": "Sami, Inari",
          "sms": "Skolt Sami",
          "sms-alt-menu": "Sami, Skolt",
          "sn": "Shona",
          "snk": "Soninke",
          "so": "Somali",
          "sog": "Sogdien",
          "sq": "Albanian",
          "sr": "Serbian",
          "sr-me": "Montenegrin",
          "srn": "Sranan Tongo",
          "srr": "Serer",
          "ss": "Swati",
          "ssy": "Saho",
          "st": "Southern Sotho",
          "stq": "Saterland Frisian",
          "str": "Straits Salish",
          "su": "Sundanese",
          "suk": "Sukuma",
          "sus": "Susu",
          "sux": "Sumerian",
          "sv": "Swedish",
          "sw": "Swahili",
          "sw-cd": "Congo Swahili",
          "swb": "Comorian",
          "syc": "Classical Syriac",
          "syr": "Syriac",
          "szl": "Silesian",
          "ta": "Tamil",
          "tce": "Southern Tutchone",
          "tcy": "Tulu",
          "te": "Telugu",
          "tem": "Timne",
          "teo": "Teso",
          "ter": "Tereno",
          "tet": "Tetum",
          "tg": "Tajik",
          "tgx": "Tagish",
          "th": "Thai",
          "tht": "Tahltan",
          "ti": "Tigrinya",
          "tig": "Tigre",
          "tiv": "Tiv",
          "tk": "Turkmen",
          "tkl": "Tokelau",
          "tkr": "Tsakhur",
          "tl": "Tagalog",
          "tlh": "Klingon",
          "tli": "Tlingit",
          "tly": "Talysh",
          "tmh": "Tamashek",
          "tn": "Tswana",
          "to": "Tongan",
          "tog": "Nyasa Tonga",
          "tpi": "Tok Pisin",
          "tr": "Turkish",
          "tru": "Turoyo",
          "trv": "Taroko",
          "trw": "Torwali",
          "ts": "Tsonga",
          "tsd": "Tsakonian",
          "tsi": "Tsimshian",
          "tt": "Tatar",
          "ttm": "Northern Tutchone",
          "ttt": "Muslim Tat",
          "tum": "Tumbuka",
          "tvl": "Tuvalu",
          "tw": "Twi",
          "twq": "Tasawaq",
          "ty": "Tahitian",
          "tyv": "Tuvinian",
          "tzm": "Central Atlas Tamazight",
          "udm": "Udmurt",
          "ug": "Uyghur",
          "ug-alt-variant": "Uighur",
          "uga": "Ugaritic",
          "uk": "Ukrainian",
          "umb": "Umbundu",
          "und": "Unknown language",
          "ur": "Urdu",
          "uz": "Uzbek",
          "vai": "Vai",
          "ve": "Venda",
          "vec": "Venetian",
          "vep": "Veps",
          "vi": "Vietnamese",
          "vls": "West Flemish",
          "vmf": "Main-Franconian",
          "vo": "Volapük",
          "vot": "Votic",
          "vro": "Võro",
          "vun": "Vunjo",
          "wa": "Walloon",
          "wae": "Walser",
          "wal": "Wolaytta",
          "war": "Waray",
          "was": "Washo",
          "wbp": "Warlpiri",
          "wo": "Wolof",
          "wuu": "Wu Chinese",
          "xal": "Kalmyk",
          "xh": "Xhosa",
          "xmf": "Mingrelian",
          "xog": "Soga",
          "yao": "Yao",
          "yap": "Yapese",
          "yav": "Yangben",
          "ybb": "Yemba",
          "yi": "Yiddish",
          "yo": "Yoruba",
          "yrl": "Nheengatu",
          "yue": "Cantonese",
          "yue-alt-menu": "Chinese, Cantonese",
          "za": "Zhuang",
          "zap": "Zapotec",
          "zbl": "Blissymbols",
          "zea": "Zeelandic",
          "zen": "Zenaga",
          "zgh": "Standard Moroccan Tamazight",
          "zh": "Chinese",
          "zh-alt-long": "Mandarin Chinese",
          "zh-alt-menu": "Chinese, Mandarin",
          "zh-hans": "Simplified Chinese",
          "zh-hans-alt-long": "Simplified Mandarin Chinese",
          "zh-hant": "Traditional Chinese",
          "zh-hant-alt-long": "Traditional Mandarin Chinese",
          "zu": "Zulu",
          "zun": "Zuni",
          "zxx": "No linguistic content",
          "zza": "Zaza"
        }
      }
    }
  }
}
`;
const icuLanguageObject = JSON.parse(icuLanguageJSON);

const getLanguageName = (languageCode: string): string => {
    return icuLanguageObject.main.en.localeDisplayNames.languages[languageCode.toLowerCase()];
};

type RedshiftPlugin = Plugin<{
    global: {
        pgClient: Client
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
        sanitizedTableName: string
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        uploadSeconds: string
        uploadMegabytes: string
        eventsToIgnore: string
        propertiesDataType: string
    }
}>

type RedshiftMeta = PluginMeta<RedshiftPlugin>

interface ParsedEvent {
    uuid: string
    eventName: string
    properties: string
    elements: string
    set: string
    set_once: string
    distinct_id: string
    team_id: number
    ip: string
    site_url: string
    timestamp: string
}

type InsertQueryValue = string | number

interface UploadJobPayload {
    batch: ParsedEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const jobs: RedshiftPlugin['jobs'] = {
    uploadBatchToRedshift: async (payload: UploadJobPayload, meta: RedshiftMeta) => {
        await insertBatchIntoRedshift(payload, meta)
    },
}

export const setupPlugin: RedshiftPlugin['setupPlugin'] = async (meta) => {
    const { global, config } = meta

    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }

    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }

    // Max Redshift insert is 16 MB: https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 10))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 600))

    global.sanitizedTableName = sanitizeSqlIdentifier(config.tableName)

    const propertiesDataType = config.propertiesDataType === 'varchar' ? 'varchar(65535)' : 'super'

    const queryError = await executeQuery(
        `CREATE TABLE IF NOT EXISTS public.${global.sanitizedTableName} (
            uuid varchar(200),
            event varchar(200),
            properties ${propertiesDataType},
            elements varchar(65535),
            set ${propertiesDataType},
            set_once ${propertiesDataType},
            timestamp timestamp with time zone,
            team_id int,
            distinct_id varchar(200),
            ip varchar(200),
            site_url varchar(200)
        );`,
        [],
        config
    )

    if (queryError) {
        throw new Error(`Unable to connect to Redshift cluster and create table with error: ${queryError.message}`)
    }

    global.buffer = createBuffer({
        limit: uploadMegabytes * 1024 * 1024,
        timeoutSeconds: uploadSeconds,
        onFlush: async (batch) => {
            await insertBatchIntoRedshift(
                { batch, batchId: Math.floor(Math.random() * 1000000), retriesPerformedSoFar: 0 },
                meta
            )
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
}

export function processEvent(event: PluginEvent) {
    // Add human-readable names for browser language
    if (event.properties && event.properties['browser_language']) {
        event.properties['browser_language_name'] = getLanguageName(String(event.properties['browser_language']));
    }

    // Return the event to ingest, return nothing to discard
    return event;
}

export async function onEvent(event: PluginEvent, { global }: RedshiftMeta) {
    const {
        event: eventName,
        properties,
        $set,
        $set_once,
        distinct_id,
        team_id,
        site_url,
        now,
        sent_at,
        uuid,
        ..._discard
    } = event

    const ip = properties?.['$ip'] || event.ip
    const timestamp = event.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (eventName === '$autocapture' && properties && '$elements' in properties) {
        const { $elements, ...props } = properties
        ingestedProperties = props
        elements = $elements
    }

    const parsedEvent = {
        uuid,
        eventName,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements || {}),
        set: JSON.stringify($set || {}),
        set_once: JSON.stringify($set_once || {}),
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp: new Date(timestamp).toISOString(),
    }

    if (!global.eventsToIgnore.has(eventName)) {
        global.buffer.add(parsedEvent)
    }
}

export const insertBatchIntoRedshift = async (payload: UploadJobPayload, { global, jobs, config }: RedshiftMeta) => {
    let values: InsertQueryValue[] = []
    let valuesString = ''

    const isSuper = config.propertiesDataType === 'super';

    for (let i = 0; i < payload.batch.length; ++i) {
        const { uuid, eventName, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp } =
            payload.batch[i]

        // if is varchar using parametrised query
        // Creates format: ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11), ($12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        // if is super type using plain text query
        // assemble the value into valueString and values is not needed
        if (isSuper) {
            valuesString += ` ('${uuid}', '${eventName}', JSON_PARSE('${properties.replace(/'/g, "''")}'), '${elements}', JSON_PARSE('${set.replace(/'/g, "''")}'), JSON_PARSE('${set_once.replace(/'/g, "''")}'), '${distinct_id}', ${team_id}, '${ip}', '${site_url}', '${timestamp}') ${i === payload.batch.length - 1 ? '' : ','}`
        } else {
            valuesString += ' ('
            for (let j = 1; j <= 11; ++j) {
                valuesString += `$${11 * i + j}${j === 11 ? '' : ', '}`
            }
            valuesString += `)${i === payload.batch.length - 1 ? '' : ','}`

            values = [
                ...values,
                ...[uuid, eventName, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp],
            ]
        }
    }

    console.log(
        `(Batch Id: ${payload.batchId}) Flushing ${payload.batch.length} event${
            payload.batch.length > 1 ? 's' : ''
        } to RedShift`
    )

    const queryError = await executeQuery(
        `INSERT INTO ${global.sanitizedTableName} (uuid, event, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp)
        VALUES ${valuesString}`,
        values,
        config
    )

    if (queryError) {
        console.error(`(Batch Id: ${payload.batchId}) Error uploading to Redshift: ${queryError.message}`)
        if (payload.retriesPerformedSoFar >= 15) {
            return
        }
        const nextRetryMs = 2 ** payload.retriesPerformedSoFar * 3000
        console.log(`Enqueued batch ${payload.batchId} for retry in ${nextRetryMs}ms`)
        await jobs
            .uploadBatchToRedshift({
                ...payload,
                retriesPerformedSoFar: payload.retriesPerformedSoFar + 1,
            })
            .runIn(nextRetryMs, 'milliseconds')
    }
}

const executeQuery = async (query: string, values: any[], config: RedshiftMeta['config']): Promise<Error | null> => {
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })

    let error: Error | null = null
    try {
        await pgClient.connect()
        await pgClient.query(query, values)
    } catch (err: any) {
        console.error(`Error executing query: ${err.message}`);
        console.error(`Query: ${query}`);
        console.error(`uuid: ${values[0]}`);
        error = err
    } finally {
        await pgClient.end()
    }

    return error
}

export const teardownPlugin: RedshiftPlugin['teardownPlugin'] = ({ global }) => {
    global.buffer.flush()
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_.]+/g, '')
}
