import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath("../"))

from src.syntax.vrt_emma_reader import VrtEmmaReader

LISTS_FOLDER = Path("../lists")
CORPUS_FILE = Path(
    "../data/vrt-with-meta-corpus-02-06-25_ordered.vrt"
)

corpus_reader = VrtEmmaReader(file_name=CORPUS_FILE)


def clean_lemma(text):
    text = text.replace("_", "")
    text = text.replace("+", "")
    text = text.replace("=", "")
    return text.strip()