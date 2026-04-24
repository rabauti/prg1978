import networkx as nx
from textwrap import wrap

from .constants import (
    ABBREVIATION_TYPES,
    ADJECTIVE_TYPES,
    ADPOSITION_TYPES,
    CAPITALIZATION_FLAGS,
    CASES,
    CONJUNCTION_TYPES,
    EMPTY_CASE,
    FEATURE_VALUE_ALIASES,
    IMPERSONAL_VOICE,
    INF_FORMS,
    MOODS,
    NEGATIONS,
    NORMAL_VERB_TENSES,
    NUMBER_FORMATS,
    NUMBERS,
    NUMERAL_TYPES,
    PERSONS,
    POS_ALIASES,
    PRONOUN_TYPES,
    PUNCTUATION_TYPES,
    SUBSTANTIVE_TYPES,
    TENSES,
    VERB_TYPES,
    VOICES,
)
from .list_utils import ListUtils

GRAPH_NAME = "SyntaxGraph"
TITLE_WRAP_WIDTH = 120

GRAPH_RANKDIR = "TB"
DEFAULT_NODE_SHAPE = "ellipse"
DEFAULT_NODE_STYLE = "filled"
DEFAULT_NODE_FILL = "lightskyblue"
HIGHLIGHT_NODE_FILL = "red"
ROOT_NODE_SHAPE = "point"
ROOT_NODE_WIDTH = "0.05"
ROOT_NODE_HEIGHT = "0.05"


class BaseDiGraph(nx.DiGraph):
    _distances_matrix = None  # matrix for node distances
    _meta = {}  # graph metadata

    def __init__(self):
        super(BaseDiGraph, self).__init__()
        self._distances_matrix = None
        self._meta = {}

    def init_distances_matrix(self):
        self._distances_matrix = {
            x[0]: x[1] for x in nx.all_pairs_shortest_path_length(self)
        }

    def get_distances_matrix(self):
        return self._distances_matrix

    def get_nodes_by_attributes(self, attrname, attrvalue):
        node_attrs = nx.get_node_attributes(self, attrname)

        if isinstance(attrvalue, (list, tuple, set)):
            wanted = set(attrvalue)
            return [node_id for node_id, value in node_attrs.items() if value in wanted]

        return [node_id for node_id, value in node_attrs.items() if value == attrvalue]

    def set_metadata(self, prop, data):
        self._meta[prop] = data

    def get_metadata(self, prop=None):
        if prop is None:
            return self._meta
        if prop in self._meta:
            return self._meta[prop]
        return None


class SyntaxGraph(BaseDiGraph):
    POS_ALIASES = POS_ALIASES
    FEATURE_VALUE_ALIASES = FEATURE_VALUE_ALIASES

    def __init__(self, stanza_syntax_layer):
        super(SyntaxGraph, self).__init__()
        for data in stanza_syntax_layer:
            if isinstance(data["id"], int):
                pos = data["upostag"]
                feats = data.get("feats") or {}
                # paneme graafi kokku
                self.add_node(
                    data["id"],
                    id=data["id"],
                    lemma=data["lemma"],
                    POS=pos,
                    POS_NORM=self.normalize_pos(pos),
                    deprel=data["deprel"],
                    form=data["form"],
                    feats=feats,
                    feature_tokens=self.normalize_feature_tokens(feats),
                    verbform=data["verbform"] if "verbform" in data else None,
                    # for conll
                    head=data["head"],
                    # start=data.start,
                    # end=data.end
                )
                self.add_edge(
                    data["id"] - data["id"] + data["head"],
                    data["id"],
                    deprel=data["deprel"],
                )
        self.init_distances_matrix()

    @classmethod
    def normalize_pos(cls, pos):
        if pos is None:
            return None
        return cls.POS_ALIASES.get(
            str(pos), cls.POS_ALIASES.get(str(pos).upper(), str(pos))
        )

    @classmethod
    def normalize_feature_tokens(cls, feats):
        if not feats:
            return set()

        tokens = set()
        items = feats.items() if isinstance(feats, dict) else []
        for key, value in items:
            key_str = str(key)
            value_str = str(value)
            tokens.update({key_str, key_str.lower(), value_str, value_str.lower()})
            alias = cls.FEATURE_VALUE_ALIASES.get(key_str, {}).get(value_str)
            if alias:
                tokens.add(alias)
        return tokens

    def iter_token_nodes(self):
        for node_id in sorted(node for node in self.nodes if node):
            yield node_id

    def get_children(self, node_id):
        return sorted(child for child in self.successors(node_id) if child)

    def get_matching_children(self, node_id, predicate=None):
        children = self.get_children(node_id)
        if predicate is None:
            return children
        return [child for child in children if predicate(self, child)]

    def get_node_pos(self, node_id, normalized=True):
        attr = "POS_NORM" if normalized else "POS"
        return self.nodes[node_id].get(attr)

    def get_feature_tokens(self, node_id):
        return self.nodes[node_id].get("feature_tokens", set())

    def _first_matching_feature(self, node_id, candidates):
        tokens = self.get_feature_tokens(node_id)
        for candidate in candidates:
            if candidate in tokens:
                return candidate
        return None

    def get_obl_info(self, sentence_obl_layer):
        obl_data = []
        for obl in sentence_obl_layer:
            obl_data.append(
                {
                    "nodes": [
                        self.get_nodes_by_attributes(
                            attrname="start", attrvalue=s.start
                        )[0]
                        for s in obl.spans
                    ],
                    "root_id": obl.root_id,
                    "root_lemma": self.nodes[obl.root_id]["lemma"],
                    "root_case": self.get_node_case(obl.root_id),
                }
            )
        return obl_data

    def get_node_case(self, node_id, not_null=True):
        """
        https://github.com/EstSyntax/EstCG/ (käänded)
        """
        case = self._first_matching_feature(
            node_id,
            CASES,
        )
        if case:
            return case
        if not_null:
            return EMPTY_CASE
        return None

    def get_node_number(self, node_id):
        """
        https://github.com/estnltk/estnltk/blob/4236f2033110d2bf20fc7f565950c0a2170f8573/estnltk/estnltk/taggers/standard/syntax/visl_rows.ipynb#L39
        :param node_id:
        :return:
        """
        return self._first_matching_feature(node_id, NUMBERS)

    def get_node_voice(self, node_id):
        return self._first_matching_feature(node_id, VOICES)

    def get_node_mood(self, node_id):
        return self._first_matching_feature(node_id, MOODS)

    def get_node_tense(self, node_id):
        return self._first_matching_feature(node_id, TENSES)

    def get_node_person(self, node_id):
        return self._first_matching_feature(node_id, PERSONS)

    def get_node_adposition_type(self, node_id):
        return self._first_matching_feature(node_id, ADPOSITION_TYPES)

    def get_node_negation(self, node_id):
        return self._first_matching_feature(node_id, NEGATIONS)

    def get_node_inf_form(self, node_id):
        return self._first_matching_feature(node_id, INF_FORMS)

    def get_node_pronoun_type(self, node_id):
        return self._first_matching_feature(node_id, PRONOUN_TYPES)

    def get_node_adjective_type(self, node_id):
        return self._first_matching_feature(node_id, ADJECTIVE_TYPES)

    def get_node_verb_type(self, node_id):
        return self._first_matching_feature(node_id, VERB_TYPES)

    def get_node_substantive_type(self, node_id):
        return self._first_matching_feature(node_id, SUBSTANTIVE_TYPES)

    def get_node_numeral_type(self, node_id):
        return self._first_matching_feature(node_id, NUMERAL_TYPES)

    def get_node_number_format(self, node_id):
        return self._first_matching_feature(node_id, NUMBER_FORMATS)

    def get_node_conjunction_type(self, node_id):
        return self._first_matching_feature(node_id, CONJUNCTION_TYPES)

    def get_node_punctuation_type(self, node_id):
        return self._first_matching_feature(node_id, PUNCTUATION_TYPES)

    def get_node_abbreviation_type(self, node_id):
        return self._first_matching_feature(node_id, ABBREVIATION_TYPES)

    def get_node_capitalized(self, node_id):
        return self._first_matching_feature(node_id, CAPITALIZATION_FLAGS)

    @staticmethod
    def _dot_quote(value):
        text = "" if value is None else str(value)
        return (
            '"'
            + text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            + '"'
        )

    def _node_colors(self, highlight=None, custom_colors=None):
        highlight = set(highlight or [])
        if not custom_colors:
            base_colors = [DEFAULT_NODE_FILL for _ in self.nodes]
        else:
            base_colors = list(custom_colors)
            if len(base_colors) < len(self.nodes):
                base_colors.extend(
                    [DEFAULT_NODE_FILL] * (len(self.nodes) - len(base_colors))
                )

        return {
            node_id: (
                HIGHLIGHT_NODE_FILL if node_id in highlight else base_colors[index]
            )
            for index, node_id in enumerate(self.nodes)
        }

    def to_dot(self, title=None, highlight=None, custom_colors=None):
        color_map = self._node_colors(highlight=highlight, custom_colors=custom_colors)
        lines = [
            f"digraph {GRAPH_NAME} {{",
            f'  rankdir="{GRAPH_RANKDIR}";',
            f'  node [shape="{DEFAULT_NODE_SHAPE}", style="{DEFAULT_NODE_STYLE}", fillcolor="{DEFAULT_NODE_FILL}"];',
        ]

        if title:
            wrapped_title = "\n".join(wrap(title, TITLE_WRAP_WIDTH))
            lines.append('  labelloc="t";')
            lines.append(f"  label={self._dot_quote(wrapped_title)};")

        for node_id, data in self.nodes(data=True):
            label = data.get("lemma", "")
            attrs = [f"label={self._dot_quote(label)}"]
            if node_id:
                attrs.extend(
                    [
                        f'shape="{DEFAULT_NODE_SHAPE}"',
                        f'style="{DEFAULT_NODE_STYLE}"',
                        f"fillcolor={self._dot_quote(color_map.get(node_id, DEFAULT_NODE_FILL))}",
                    ]
                )
            else:
                attrs.extend(
                    [
                        f'shape="{ROOT_NODE_SHAPE}"',
                        f'width="{ROOT_NODE_WIDTH}"',
                        f'height="{ROOT_NODE_HEIGHT}"',
                    ]
                )
            lines.append(f"  {node_id} [{', '.join(attrs)}];")

        for source, target, data in self.edges(data=True):
            label = data.get("deprel", "")
            lines.append(f"  {source} -> {target} [label={self._dot_quote(label)}];")

        lines.append("}")
        return "\n".join(lines) + "\n"

    def is_verb_normal(self, verb):
        """
        verb on "normaalne", kui pole umbisikuline ja verbi aeg on
        past' või 'impf' või 'pres'
        """
        # kui on umbisikuline
        tokens = self.get_feature_tokens(verb)
        if IMPERSONAL_VOICE in tokens:
            return False

        # tense pole past, impf, pres
        if not len(ListUtils.list_intersection(list(NORMAL_VERB_TENSES), list(tokens))):
            return False
        return True
