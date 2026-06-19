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

        if mode not in ("graph", "text"):
            raise Exception(f"Unknown mode {mode}")

        header = None
        current_sentence = []
        sent_id = None
        doc_attrs = {}

        with open(self._file_name, "r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                line = raw_line.rstrip("\r\n")

                if not line:
                    continue

                # Read the column header.
                if line_no == 1:
                    if line.startswith("#"):
                        header = line[1:]
                    else:
                        header = line

                    header = header.strip().split()
                    continue

                # Process structural tags.
                if line.startswith("<"):
                    if line.startswith("<sentence"):
                        match = self._SENTENCE_OPEN_RE.match(line)
                        sent_id = match.group(1) if match else None
                        current_sentence = []

                    elif line == "</sentence>":
                        sentence_text = self._make_sentence_text(
                            current_sentence
                        )

                        if mode == "graph":
                            graph = UDSyntaxGraph(current_sentence)
                            graph.set_metadata("doc", doc_attrs)
                            graph.set_metadata("sent_id", sent_id)
                            graph.set_metadata("text", sentence_text)

                            yield sent_id, graph
                        else:
                            yield sent_id, sentence_text

                        current_sentence = []
                        sent_id = None

                    elif line.startswith("<text"):
                        doc_attrs = dict(
                            self._TEXT_ATTR_RE.findall(line)
                        )

                    elif (
                        line.startswith("</text")
                        or line == "<corpus>"
                        or line == "</corpus>"
                        or line.startswith("<corpus")
                    ):
                        doc_attrs = {}

                    else:
                        self.log_info(
                            f"Ignoring unknown tag on line "
                            f"{line_no}: {line}"
                        )

                    continue

                if header is None:
                    raise Exception(
                        f"Token row found before header line "
                        f"({line_no}) in {self._file_name}"
                    )

                row = line.split("\t")

                if len(row) != len(header):
                    self.log_error(line)

                    for index, value in enumerate(row):
                        column_name = (
                            header[index]
                            if index < len(header)
                            else "<extra column>"
                        )
                        print(column_name, index + 1, value)

                    raise Exception(
                        f"Wrong column count on line {line_no} "
                        f"in {self._file_name}: expected "
                        f"{len(header)}, got {len(row)}"
                    )

                fields = dict(zip(header, row))

                node_id = int(fields["word_id"])
                head = int(fields["DependencyHead"])

                if head == node_id:
                    # The source marks a root using a self-loop.
                    # Convert it to the standard CoNLL-U root head.
                    head = 0

                try:
                    pos_start, pos_end = map(
                        int,
                        fields["word_tokens"].split("-", maxsplit=1),
                    )
                except (TypeError, ValueError) as error:
                    raise ValueError(
                        f"Invalid word_tokens value on line {line_no}: "
                        f"{fields.get('word_tokens')!r}"
                    ) from error

                current_sentence.append(
                    {
                        "id": node_id,
                        "form": fields["word"],
                        "lemma": fields["Lemma"],
                        "upostag": fields["coarseValue"],
                        "deprel": fields["DependencyType"],
                        "head": head,
                        "feats": self._parse_feats(fields["value"]),
                        "verbform": (
                            None
                            if fields["verbForm"] == "_"
                            else fields["verbForm"]
                        ),
                        "pos_start": pos_start,
                        "pos_end": pos_end,
                    }
                )

    @staticmethod
    def _make_sentence_text(tokens):
        """
        Reconstruct sentence text using token character positions.

        A space is inserted only when there is a gap between the end of
        the previous token and the beginning of the current token.

        Examples:
            Hello + ,       -> Hello,
            Hello, + world  -> Hello, world
            world + !       -> world!

        The exact gap length is preserved when it is greater than zero.
        No leading whitespace is added before the first token.
        """
        if not tokens:
            return ""

        ordered_tokens = sorted(
            tokens,
            key=lambda token: (
                token["pos_start"],
                token["pos_end"],
                token["id"],
            ),
        )

        sentence_parts = []
        previous_pos_end = None

        for token in ordered_tokens:
            form = token.get("form", "")
            pos_start = token["pos_start"]
            pos_end = token["pos_end"]

            if previous_pos_end is not None:
                gap = pos_start - previous_pos_end

                if gap > 0:
                    sentence_parts.append(" " * gap)

            sentence_parts.append(form)

            if previous_pos_end is None:
                previous_pos_end = pos_end
            else:
                # Keep the furthest end position if token spans overlap.
                previous_pos_end = max(previous_pos_end, pos_end)

        return "".join(sentence_parts)

    @staticmethod
    def _parse_feats(value):
        """
        Parse a UD-style feature string into a dictionary.

        Example:
            "Case=Nom|Number=Sing"
            -> {"Case": "Nom", "Number": "Sing"}

        "_" or an empty value becomes an empty dictionary.
        """
        if not value or value == "_":
            return {}

        return {
            key: feature_value
            for part in value.split("|")
            if "=" in part
            for key, feature_value in [part.split("=", 1)]
        }