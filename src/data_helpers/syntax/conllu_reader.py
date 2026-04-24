"""
conllu format
"""

from io import open
from conllu import parse_incr

from .base_reader import BaseReader
from .constants import MODE_GRAPH, READER_MODES
from .syntax_graph import SyntaxGraph


class CoNNLUReader(BaseReader):

    __FILE = None

    def __init__(self, file_name):
        self.__FILE = file_name
        super().__init__()

    def get_sentences(self, mode=MODE_GRAPH):
        self.log_info("Reading sentences in progress.")
        if mode not in READER_MODES:
            raise Exception("Unknown mode %s", mode)
        data_file = open(self.__FILE, "r", encoding="utf-8")
        for tokenlist in parse_incr(data_file):
            # print(tokenlist)
            # print(vars(tokenlist))
            if mode == MODE_GRAPH:
                g = SyntaxGraph(tokenlist)
                for k in tokenlist.metadata.keys():
                    g.set_metadata(k, tokenlist.metadata[k])
                yield tokenlist.metadata["sent_id"], g
            else:
                yield tokenlist.metadata["sent_id"], tokenlist.metadata["text"]


__all__ = ["CoNNLUReader"]
