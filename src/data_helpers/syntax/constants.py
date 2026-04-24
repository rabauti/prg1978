EMPTY_CASE = "<puudub>"
MODE_GRAPH = "graph"
MODE_TEXT = "text"
READER_MODES = (
    MODE_GRAPH,
    MODE_TEXT,
)

DEPREL_OBL = "obl"
SUBJECT_DEPRELS = (
    "nsubj",
    "csubj",
)
COMPOUND_PARTICLE_DEPREL = "compound:prt"
VERB_POS = "V"

POS_ALIASES = {
    "A": "A",
    "ADJ": "A",
    "ADP": "K",
    "ADV": "D",
    "AUX": "V",
    "CCONJ": "J",
    "DET": "P",
    "D": "D",
    "J": "J",
    "K": "K",
    "NOUN": "S",
    "NUM": "N",
    "P": "P",
    "PRON": "P",
    "PROPN": "S",
    "PUNCT": "Z",
    "SCONJ": "J",
    "S": "S",
    "VERB": "V",
    "V": "V",
    "Z": "Z",
}

FEATURE_VALUE_ALIASES = {
    "Case": {
        "Abe": "abes",
        "Abl": "abl",
        "Add": "adit",
        "Ade": "ad",
        "All": "all",
        "Com": "kom",
        "Ela": "el",
        "Ess": "es",
        "Gen": "gen",
        "Ill": "ill",
        "Ine": "in",
        "Nom": "nom",
        "Par": "part",
        "Tra": "tr",
    },
    "Degree": {
        "Cmp": "comp",
        "Pos": "pos",
        "Sup": "super",
    },
    "Mood": {
        "Cnd": "cond",
        "Imp": "imper",
        "Ind": "indic",
        "Qot": "quot",
    },
    "Number": {
        "Plur": "pl",
        "Sing": "sg",
    },
    "Person": {
        "1": "ps1",
        "2": "ps2",
        "3": "ps3",
    },
    "Tense": {
        "Past": "past",
        "Pres": "pres",
    },
    "VerbForm": {
        "Fin": "fin",
        "Ger": "ger",
        "Inf": "inf",
        "Part": "partic",
        "Sup": "sup",
    },
    "Voice": {
        "Act": "af",
        "Pass": "ps",
    },
}

CASES = (
    "nom",
    "gen",
    "part",
    "adit",
    "ill",
    "in",
    "el",
    "all",
    "ad",
    "abl",
    "tr",
    "term",
    "es",
    "abes",
    "kom",
)

LOCATIVE_CASES = (
    "adit",
    "ill",
    "in",
    "el",
    "all",
    "ad",
    "abl",
)

NUMBERS = (
    "sg",
    "pl",
)

VOICES = (
    "imps",
    "ps",
)

IMPERSONAL_VOICE = "imps"

MOODS = (
    "indic",
    "cond",
    "imper",
    "quot",
)

TENSES = (
    "pres",
    "past",
    "impf",
)

NORMAL_VERB_TENSES = (
    "past",
    "impf",
    "pres",
)

PERSONS = (
    "ps1",
    "ps2",
    "ps3",
)

ADPOSITION_TYPES = (
    "pre",
    "post",
)

NEGATIONS = (
    "af",
    "neg",
)

INF_FORMS = (
    "sup",
    "inf",
    "ger",
    "partic",
)

PRONOUN_TYPES = (
    "pos",
    "det",
    "refl",
    "dem",
    "inter_rel",
    "pers",
    "rel",
    "rec",
    "indef",
)

ADJECTIVE_TYPES = (
    "pos",
    "comp",
    "super",
)

VERB_TYPES = (
    "main",
    "mod",
    "aux",
)

SUBSTANTIVE_TYPES = (
    "prop",
    "com",
)

NUMERAL_TYPES = (
    "card",
    "ord",
)

NUMBER_FORMATS = (
    "l",
    "roman",
    "digit",
)

CONJUNCTION_TYPES = (
    "crd",
    "sub",
)

PUNCTUATION_TYPES = (
    "Col",
    "Com",
    "Cpr",
    "Cqu",
    "Csq",
    "Dsd",
    "Dsh",
    "Ell",
    "Els",
    "Exc",
    "Fst",
    "Int",
    "Opr",
    "Oqu",
    "Osq",
    "Quo",
    "Scl",
    "Sla",
    "Sml",
)

ABBREVIATION_TYPES = (
    "adjectival",
    "adverbial",
    "nominal",
    "verbal",
)

CAPITALIZATION_FLAGS = ("cap",)
