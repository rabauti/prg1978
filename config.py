from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
DATA_HELPERS_DIR = SRC_DIR / "data_helpers"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
DATA_DIR = PROJECT_ROOT / "data"
TMP_DIR = PROJECT_ROOT / "tmp"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_DERIVED_DIR = DATA_DIR / "derived"
DATA_DERIVED_STATS_DIR = DATA_DERIVED_DIR / "stats"
DATA_DERIVED_QUERY_RESULTS_DIR = DATA_DERIVED_DIR / "query_results"

SOURCE_CONLLU_FILE = DATA_RAW_DIR / "test.conllu"
METADATA_TSV_FILE = DATA_RAW_DIR / "metadata.tsv"
METADATA_TSV_DELIMITER = ";"

LEMMA_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_lemmas.tsv"
VERB_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_verbs.tsv"
VERB_COMPOUND_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_verbs_compound.tsv"

VERB_XCOMP_INF_RESULTS_DIR = DATA_DERIVED_QUERY_RESULTS_DIR
MINEMA_KOHAKAANDED_RESULTS_FILE = (
    DATA_DERIVED_QUERY_RESULTS_DIR / "minema_kohakaanded.tsv"
)


def _apply_local_overrides():
    try:
        import config_local as _config_local
    except ImportError:
        return

    for name in dir(_config_local):
        if name.isupper():
            globals()[name] = getattr(_config_local, name)


_apply_local_overrides()
