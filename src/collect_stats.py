from pathlib import Path

from config import (
    LEMMA_STATS_FILE,
    SOURCE_CONLLU_FILE,
    VERB_COMPOUND_STATS_FILE,
    VERB_STATS_FILE,
)

try:
    from .data_helpers.stats import collect_lemma_pos_stats, collect_verb_stats
    from .data_helpers.syntax.conllu_reader import CoNNLUReader
except ImportError:
    from data_helpers.stats import collect_lemma_pos_stats, collect_verb_stats
    from data_helpers.syntax.conllu_reader import CoNNLUReader


def collect_lemmas(source_file=SOURCE_CONLLU_FILE, output_file=LEMMA_STATS_FILE):
    reader = CoNNLUReader(file_name=source_file)
    stats = collect_lemma_pos_stats(reader)
    dataframe = stats.to_dataframe()
    stats.save_tsv(Path(output_file))
    return stats, dataframe


def collect_verbs(
    source_file=SOURCE_CONLLU_FILE,
    plain_output_file=VERB_STATS_FILE,
    compound_output_file=VERB_COMPOUND_STATS_FILE,
):
    reader = CoNNLUReader(file_name=source_file)
    stats = collect_verb_stats(reader)
    plain_dataframe = stats.plain_dataframe()
    compound_dataframe = stats.compound_dataframe()
    stats.save_tsv(Path(plain_output_file), Path(compound_output_file))
    return stats, plain_dataframe, compound_dataframe


def collect_all(source_file=SOURCE_CONLLU_FILE):
    lemma_stats, lemma_dataframe = collect_lemmas(
        source_file=source_file,
        output_file=LEMMA_STATS_FILE,
    )
    verb_stats, verb_dataframe, compound_dataframe = collect_verbs(
        source_file=source_file,
        plain_output_file=VERB_STATS_FILE,
        compound_output_file=VERB_COMPOUND_STATS_FILE,
    )
    return {
        "lemma_stats": lemma_stats,
        "lemma_dataframe": lemma_dataframe,
        "verb_stats": verb_stats,
        "verb_dataframe": verb_dataframe,
        "verb_compound_dataframe": compound_dataframe,
    }


def main():
    results = collect_all()
    print(f"Lemmas total: {results['lemma_stats'].total_tokens}")
    print(f"Lemma stats written to {LEMMA_STATS_FILE}")
    print(f"Verbs total: {results['verb_stats'].total_verbs}")
    print(f"Verb stats written to {VERB_STATS_FILE}")
    print(f"Compound verb stats written to {VERB_COMPOUND_STATS_FILE}")


if __name__ == "__main__":
    main()
