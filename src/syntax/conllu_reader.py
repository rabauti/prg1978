from tqdm import tqdm

from io import open
from conllu import parse_incr

from .base_reader import BaseReader
from .syntax_graph_ud import UDSyntaxGraph


class CoNNLUReader(BaseReader):
    """
    conllu format
    """
    _file_name = None

    def __init__(self, file_name):
        self._file_name = file_name
        super().__init__()

    def get_sentences(self, mode='graph'):
        print(self._file_name)
        self.log_info('Reading sentences in progress.')
        if mode not in ['graph', 'text']:
            raise Exception("Unknown mode %s", mode)
        # global line counter
        # count = 0
        # doc = None
        data_file = open(self._file_name, "r", encoding="utf-8")
        for tokenlist in parse_incr(data_file):
            # print(tokenlist)
            # print(vars(tokenlist))
            if mode == 'graph':
                g = UDSyntaxGraph(tokenlist)
                for k in tokenlist.metadata.keys():
                    g.set_metadata(k, tokenlist.metadata[k])
                yield tokenlist.metadata['sent_id'], g
            else:
                yield tokenlist.metadata['sent_id'], tokenlist.metadata['text']
