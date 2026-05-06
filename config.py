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
LOCAL_CONFIG_FILE = PROJECT_ROOT / "config_local.py"

SOURCE_CONLLU_FILE = DATA_RAW_DIR / "test.conllu"
METADATA_TSV_FILE = DATA_RAW_DIR / "metadata.tsv"
METADATA_TSV_DELIMITER = ";"

LEMMA_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_lemmas.tsv"
VERB_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_verbs.tsv"
VERB_COMPOUND_STATS_FILE = DATA_DERIVED_STATS_DIR / "conllu_verbs_compound.tsv"


def _apply_local_overrides():
    if not LOCAL_CONFIG_FILE.exists():
        return

    namespace = dict(globals())
    namespace["__file__"] = str(LOCAL_CONFIG_FILE)
    namespace["__name__"] = "config_local"

    exec(
        compile(
            LOCAL_CONFIG_FILE.read_text(encoding="utf-8"),
            str(LOCAL_CONFIG_FILE),
            "exec",
        ),
        namespace,
    )

    for name, value in namespace.items():
        if name.isupper():
            globals()[name] = value


_apply_local_overrides()
