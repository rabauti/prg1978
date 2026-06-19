import re
 
from .base_reader import BaseReader
from .syntax_graph_ud import UDSyntaxGraph
 
"""
EMMA .vrt format
 
A tab-separated positional-attribute table (one token per line), with
column names given by a "#"-prefixed header line, interleaved with
structural tags: <text ...> ... </text> wrapping one or more
<sentence id="..."> ... </sentence> blocks.
 
Example (see data/emma-example.vrt):
    #word word_id word_tokens animacy aspect case ... DependencyType flavor DependencyHead error_type
    <text id="232" ...>
    <sentence id="232">
    Kitukas 1   0-7 _ _ Nom _ ... nsubj   basic   3   _
    ...
    </sentence>
    </text>
 
The columns actually used to build the syntax graph:
    word           -> form
    word_id        -> id (token's sequence number in the sentence)
    Lemma          -> lemma
    coarseValue    -> upostag (UD POS tag, e.g. NOUN, VERB, AUX)
    value          -> feats (UD-style "Case=Nom|Number=Sing" string, or "_")
    verbForm       -> verbform
    DependencyType -> deprel (UD relation, e.g. nsubj, root, obl)
    DependencyHead -> head (word_id of the head token; in the raw file root
                       tokens point to themselves rather than to 0, this is
                       normalized to 0 - the standard CoNLL-U convention -
                       while parsing)
 
The header is read from the file itself rather than hard-coded, so the
reader doesn't break if unrelated columns are added/reordered.
"""
 
 
class VrtEmmaReader(BaseReader):
    _file_name = None
 
    _SENTENCE_OPEN_RE = re.compile(r'^<sentence(?:\s+id="([^"]*)")?[^>]*>$')
    _TEXT_ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
 
    def __init__(self, file_name):
        self._file_name = file_name
        super().__init__()
 
    def get_sentences(self, mode="graph"):
        print(self._file_name)
        self.log_info("Reading sentences in progress.")
        if mode not in ["graph", "text"]:
            raise Exception("Unknown mode %s", mode)
 
        header = None
        current_sentence = []
        sent_id = None
        doc_attrs = {}
 
        with open(self._file_name, "r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
 
                if line_no==1:
                    # header line, e.g. "#word word_id word_tokens ... error_type"
                    if line.startswith("#"):
                        header = line[1:]
                    else:
                        header = line[0:]
                    header = header.strip().split()
                    continue
 
                if line.startswith("<"):
                    if line.startswith("<sentence"):
                        m = self._SENTENCE_OPEN_RE.match(line)
                        sent_id = m.group(1) if m else None
                        current_sentence = []
                    elif line == "</sentence>":
                        g = UDSyntaxGraph(current_sentence)
                        g.set_metadata("doc", doc_attrs)
                        g.set_metadata("sent_id", sent_id)
                        if mode == "graph":
                            yield sent_id, g
                        else:
                            text = " ".join(t["form"] for t in current_sentence)
                            yield sent_id, text
                    elif line.startswith("<text"):
                        doc_attrs = dict(self._TEXT_ATTR_RE.findall(line))
                    elif line.startswith("</text") or line in ("<corpus>", "</corpus>") or line.startswith("<corpus"):
                        # document/corpus-level wrapper tags, nothing to do
                        pass
                    else:
                        self.log_info(f"Ignoring unknown tag on line {line_no}: {line}")
                    continue
 
                if header is None:
                    raise Exception(
                        f"Token row found before header line ({line_no}) in {self._file_name}"
                    )
 
                row = line.split("\t")
                if len(row) != len(header):
                    self.log_error(line)
                    for i, t in enumerate(row):
                        print(header[i], i+1, t)
                    raise Exception(
                        f"Wrong column count on line {line_no} in {self._file_name}: "
                        f"expected {len(header)}, got {len(row)}"
                    )
                fields = dict(zip(header, row))
 
                node_id = int(fields["word_id"])
                head = int(fields["DependencyHead"])
                if head == node_id:
                    # raw file marks the root with a self-loop (head == id);
                    # normalize to the standard CoNLL-U root convention (0)
                    head = 0
 
                current_sentence.append(
                    {
                        "id": node_id,
                        "form": fields["word"],
                        "lemma": fields["Lemma"],
                        "upostag": fields["coarseValue"],
                        "deprel": fields["DependencyType"],
                        "head": head,
                        "feats": self._parse_feats(fields["value"]),
                        "verbform": None if fields["verbForm"] == "_" else fields["verbForm"],
                    }
                )
 
    @staticmethod
    def _parse_feats(value):
        """Parses a UD-style "Key1=Val1|Key2=Val2" string into a dict; "_" -> {}."""
        if not value or value == "_":
            return {}
        return dict(part.split("=", 1) for part in value.split("|") if "=" in part)
 
 