import re
from tqdm import tqdm


from .base_reader import BaseReader
from .syntax_graph import SyntaxGraph

"""
0 word - tokens
1 longtag - morphologycal features, same as extended features
2 lempos - lemma + POS
3 features - paradigm
4 root_tokens - in case of a compound bussijuht ('bus driver'): bussi juht
5 root - in case of a compound bussijuht ('bus driver'): bussi_juht
6 ending - sufix
7 clitic - clitics -ki and -gi
8 extended feat - same as longtag, comes from UD
9 punctuation_type - marks punctuation types, e.g.
    Quo ("), Col (:), Scl(;). It covers only 24 most commonly used punctuation symbols,
    so it's not an extensive feature (link)
10 pronoun_type - marks pronon types based on this file
11 finite_verb - boolean: marks if the verb is finite (True) or infinite (False)
12 subcat - adds subcategorization info based on this file
13 syn_id - the sequence number of the word itself
14 syn_head - the sequence number of the head word
15 syn_rel - UD categories (syntactic relations)
16 head_word - surface form of the head word
17 head_lemma - lemma of the head word
18 head_tag - part of speech of the head word
19 head_features - form categories of the head word
20 head_syn_rel - UD dependency label of the head word
"""


class Nc21Reader(BaseReader):
    __FILE = None

    def __init__(self, file_name):
        self.__FILE = file_name
        super().__init__()

    def get_sentences(self, mode="graph"):
        print(self.__FILE)
        self.log_info("Reading sentences in progress.")
        if mode not in ["graph", "text"]:
            raise Exception("Unknown mode %s", mode)
        # global line counter
        count = 0
        doc = None
        with open(self.__FILE) as f:
            # current sentence data
            current_sentence = []
            # sentence graph object
            # current collection id
            col_id = 0
            # prev collection id
            # prev_col = None
            # global sentence id - collid_sentensespanstart_sentencecountincollection
            global_sent_id = None
            # sentence counter in collection
            # coll_sentence_counter = 0
            # sentence_span_start = 0
            # total lines in TSV file for progressbar calculations
            total_lines = self.count_lines()
            for line in tqdm(f, total=total_lines, desc="TSV lines"):
                count += 1
                line = line.strip("\r\n")
                row = line.split("\t")
                if len(row) == 1 and line.startswith("<"):
                    if line == "<s>":
                        col_id += 1
                        current_sentence = []
                        global_sent_id = col_id
                    elif line.startswith("<doc "):
                        doc = re.sub(r"^<doc +", "", line)
                    elif line == "</s>":
                        g = SyntaxGraph(current_sentence)
                        if mode == "graph":
                            g.set_metadata("doc", doc)
                            yield global_sent_id, g
                        else:
                            yield global_sent_id, current_sentence_text
                    continue
                elif not len(row) == 21:
                    self.log_error(line)
                    raise Exception(
                        f"Wrong columns number line number {count} in TSV {self.__FILE}"
                    )
                data = {}
                # if not prev_col == col_id:
                #    coll_sentence_counter = 0
                # prev_col = col_id
                # prev_global_sent_id = global_sent_id
                # prev_sentence_span_start = sentence_span_start
                data["id"] = int(row[13])
                data["form"] = row[0]
                data["lemma"] = "-".join(row[2].split("-")[:-1])
                data["upostag"] = row[1].split(".")[0]
                data["deprel"] = row[15]
                data["head"] = int(row[14])
                data["feats"] = {value: value for value in row[8].split("_")}
                data["verbform"] = None
                if data["upostag"] in "V":
                    data["verbform"] = row[1].split(".")[-1]

                    # fields for conll

                # print(data)
                # data['start'] = None
                # data['text'] = None

                current_sentence.append(data)

    def count_lines(self):
        def blocks(files, size=65536):
            while True:
                b = files.read(size)
                if not b:
                    break
                yield b

        with open(self.__FILE, "r", encoding="utf-8", errors="ignore") as f:
            return sum(bl.count("\n") for bl in blocks(f))
