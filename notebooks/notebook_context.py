import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath("../"))

from src.syntax.conllu_reader import CoNNLUReader

LISTS_FOLDER = Path("../lists")
CORPUS_FILE = Path(
    "../data/Model2Eesti-keele-kui-teise-keele-kooliõpikute-lausete-korpus-2021.conllu"
)

corpus_reader = CoNNLUReader(file_name=CORPUS_FILE)


def clean_lemma(text):
    text = text.replace("_", "")
    text = text.replace("+", "")
    text = text.replace("=", "")
    return text.strip()
