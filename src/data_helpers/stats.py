from collections import Counter
from dataclasses import dataclass

import pandas as pd

from .syntax.constants import COMPOUND_PARTICLE_DEPREL, VERB_POS


@dataclass
class LemmaPosStats:
    counts: Counter
    total_tokens: int

    def to_dataframe(self):
        rows = [
            {"lemma": lemma, "POS": pos, "total": total}
            for (lemma, pos), total in self.counts.items()
        ]
        df = pd.DataFrame(rows, columns=["lemma", "POS", "total"])
        if not df.empty:
            df.sort_values(["total"], ascending=[False], inplace=True)
        return df

    def save_tsv(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.to_dataframe().to_csv(path, index=None, sep="\t")


@dataclass
class VerbStats:
    plain_counts: Counter
    compound_counts: Counter
    total_verbs: int

    def plain_dataframe(self):
        rows = [
            {"verb": verb, "total": total} for verb, total in self.plain_counts.items()
        ]
        df = pd.DataFrame(rows, columns=["verb", "total"])
        if not df.empty:
            df.sort_values(["total"], ascending=[False], inplace=True)
        return df

    def compound_dataframe(self):
        rows = [
            {"verb": verb, "compound": compound, "total": total}
            for (verb, compound), total in self.compound_counts.items()
        ]
        df = pd.DataFrame(rows, columns=["verb", "compound", "total"])
        if not df.empty:
            df.sort_values(["total"], ascending=[False], inplace=True)
        return df

    def save_tsv(self, plain_path, compound_path):
        plain_path.parent.mkdir(parents=True, exist_ok=True)
        self.plain_dataframe().to_csv(plain_path, index=None, sep="\t")
        self.compound_dataframe().to_csv(compound_path, index=None, sep="\t")


def _iter_sentence_graphs(source):
    if hasattr(source, "get_sentences"):
        yield from source.get_sentences()
        return
    yield from source


def collect_lemma_pos_stats(source):
    counts = Counter()
    total_tokens = 0

    for _, graph in _iter_sentence_graphs(source):
        for node_id in graph.iter_token_nodes():
            key = (graph.nodes[node_id]["lemma"], graph.get_node_pos(node_id))
            counts[key] += 1
            total_tokens += 1

    return LemmaPosStats(counts=counts, total_tokens=total_tokens)


def collect_verb_stats(source):
    plain_counts = Counter()
    compound_counts = Counter()
    total_verbs = 0

    for _, graph in _iter_sentence_graphs(source):
        compound_nodes = {
            node_id
            for node_id in graph.iter_token_nodes()
            if graph.nodes[node_id].get("deprel") == COMPOUND_PARTICLE_DEPREL
        }
        verb_nodes = [
            node_id
            for node_id in graph.iter_token_nodes()
            if graph.get_node_pos(node_id) == VERB_POS
        ]

        for verb_id in verb_nodes:
            total_verbs += 1
            lemma = graph.nodes[verb_id]["lemma"]
            plain_counts[lemma] += 1

            compounds = [
                child_id
                for child_id in graph.get_children(verb_id)
                if child_id in compound_nodes
            ]
            compound_lemma = ", ".join(
                graph.nodes[node_id]["lemma"] for node_id in compounds
            )
            compound_counts[(lemma, compound_lemma)] += 1

    return VerbStats(
        plain_counts=plain_counts,
        compound_counts=compound_counts,
        total_verbs=total_verbs,
    )
