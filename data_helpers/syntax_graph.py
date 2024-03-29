import networkx as nx
from collections import defaultdict

# needed for draw_graph function
from networkx.drawing.nx_agraph import graphviz_layout
import matplotlib.pyplot as plt
from textwrap import wrap

# needed for draw_graph function

import matplotlib.image as mpimg
import pygraphviz as pgv

from .utils import ListUtils


class BaseDiGraph(nx.DiGraph):
    _distances_matrix = None  # matrix for node distances
    _meta = {}  # graph metadata

    def __init__(self):
        super(BaseDiGraph, self).__init__()

    def init_distances_matrix(self):
        self._distances_matrix = {
            x[0]: x[1] for x in nx.all_pairs_shortest_path_length(self)
        }

    def get_distances_matrix(self):
        return self._distances_matrix

    def get_nodes_by_attributes(self, attrname, attrvalue):
        nodes = defaultdict(list)
        {nodes[v].append(k) for k, v in nx.get_node_attributes(self, attrname).items()}
        if attrvalue in nodes:
            return dict(nodes)[attrvalue]
        return []

    def set_metadata(self, prop, data):
        self._meta[prop] = data

    def get_metadata(self, prop=None):
        if prop is None:
            return self._meta
        if prop in self._meta:
            return self._meta[prop]
        return None


class SyntaxGraph(BaseDiGraph):

    def __init__(self, stanza_syntax_layer):
        super(SyntaxGraph, self).__init__()
        for data in stanza_syntax_layer:
            if isinstance(data["id"], int):
                # paneme graafi kokku
                self.add_node(
                    data["id"],
                    id=data["id"],
                    lemma=data["lemma"],
                    POS=data["upostag"],
                    deprel=data["deprel"],
                    form=data["form"],
                    feats=data["feats"],
                    verbform=data["verbform"],
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
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in (
                "nom",  # nimetav
                "gen",  # omastav
                "part",  # osastav
                "adit",  # lyh sisse
                "ill",  # sisse
                "in",  # sees
                "el",  # seest
                "all",  # alale
                "ad",  # alal
                "abl",  # alalt
                "tr",  # saav
                "term",  # rajav
                "es",  # olev
                "abes",  # ilma#
                "kom",  # kaasa#
            ):
                return attr
        if not_null:
            return "<puudub>"
        return None

    def get_node_number(self, node_id):
        """
        https://github.com/estnltk/estnltk/blob/4236f2033110d2bf20fc7f565950c0a2170f8573/estnltk/estnltk/taggers/standard/syntax/visl_rows.ipynb#L39
        :param node_id:
        :return:
        """
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in (
                "sg",  # ainsus
                "pl",  # mitmus
            ):
                return attr
        return None

    def get_node_voice(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("imps", "ps"):
                return attr
        return None

    def get_node_mood(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("indic", "cond", "imper", "quot"):
                return attr
        return None

    def get_node_tense(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("pres", "past", "impf"):
                return attr
        return None

    def get_node_person(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("ps1", "ps2", "ps3"):
                return attr
        return None

    def get_node_adposition_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("pre", "post"):
                return attr
        return None

    def get_node_negation(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("af", "neg"):
                return attr
        return None

    def get_node_inf_form(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("sup", "inf", "ger", "partic"):
                return attr
        return None

    def get_node_pronoun_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in (
                "pos",
                "det",
                "refl",
                "dem",
                "inter_rel",
                "pers",
                "rel",
                "rec",
                "indef",
            ):
                return attr
        return None

    def get_node_adjective_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("pos", "comp", "super"):
                return attr
        return None

    def get_node_verb_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("main", "mod", "aux"):
                return attr
        return None

    def get_node_substantive_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("prop", "com"):
                return attr
        return None

    def get_node_numeral_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("card", "ord"):
                return attr
        return None

    def get_node_number_format(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("l", "roman", "digit"):
                return attr
        return None

    def get_node_conjunction_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("crd", "sub"):
                return attr
        return None

    def get_node_punctuation_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in (
                "Col",
                "Com",
                "Cpr",
                "Cqu",
                "Csq",
                "Dsd",
                "Dsh",
                "Ell",
                "Els",
                "Exc",
                "Fst",
                "Int",
                "Opr",
                "Oqu",
                "Osq",
                "Quo",
                "Scl",
                "Sla",
                "Sml",
            ):
                return attr
        return None

    def get_node_abbreviation_type(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("adjectival", "adverbial", "nominal", "verbal"):
                return attr
        return None

    def get_node_capitalized(self, node_id):
        feats = self.nodes[node_id]["feats"]
        for attr in feats:
            if attr in ("cap"):
                return attr
        return None

    def draw_graph(self, **kwargs):
        """
        Puu/graafi joonistamine
        tipp - lemma
        serv - deprel

        title string    Graafi pealkiri
        filename string Failinimi kuhu joonis salvestatakse
        highlight array of integers     tippude id, d mis värvitakse joonisel punaseks
        """
        title = None
        filename = None
        custom_colors = None
        highlight = []
        if "title" in kwargs:
            title = kwargs["title"]

        if "filename" in kwargs:
            filename = kwargs["filename"]

        if "highlight" in kwargs:
            highlight = kwargs["highlight"]

        if "custom_colors" in kwargs:
            custom_colors = kwargs["custom_colors"]

        if not custom_colors:
            colors = ["lightskyblue" for node in self]
        else:
            colors = custom_colors
        # soovitud tipud punaseks

        color_map = [
            "red" if node in highlight else colors[i]
            for (i, node) in enumerate(self.nodes)
        ]

        # print (color_map)
        # joonise suurus, et enamik puudest ära mahuks
        plt.rcParams["figure.figsize"] = (18.5, 10.5)

        # pealkiri
        if title:
            title = "\n".join(wrap(title, 120))
            plt.title(title)

        pos = graphviz_layout(self, prog="dot")
        labels = nx.get_node_attributes(self, "lemma")
        nx.draw(
            self,
            pos,
            cmap=plt.get_cmap("jet"),
            labels=labels,
            with_labels=True,
            node_color=color_map,
        )
        edge_labels = nx.get_edge_attributes(self, "deprel")
        nx.draw_networkx_edge_labels(self, pos, edge_labels)

        # kui failinimi, siis salvestame faili
        # kui pole, siis joonistame väljundisse
        if filename:
            plt.savefig(f"{filename}.png", dpi=100)
        else:
            plt.show()
        plt.clf()

    def draw_graph2(
        self,
        filename=None,
        highlight=[],
    ):

        # Create a default color for all nodes and a highlight color for selected nodes
        default_color = "lightskyblue"
        highlight_color = "red"

        # Create a new directed graph using pygraphviz
        G = pgv.AGraph(strict=True, directed=True)

        # Add nodes with 'label' attribute set to the 'lemma' from the original graph
        for node_id, data in self.nodes(data=True):
            label = data.get(
                "lemma", ""
            )  # Default to empty string if lemma is not present
            color = highlight_color if node_id in highlight else default_color
            if node_id:
                G.add_node(
                    node_id,
                    label=label,
                    shape="ellipse",
                    style="filled",
                    fillcolor=color,
                )
            else:
                G.add_node(
                    node_id, label=label, shape="none", style="none", fillcolor=color
                )

        # Add edges with 'label' attribute set to the 'deprel' from the original graph
        for source, target, data in self.edges(data=True):
            label = data.get(
                "deprel", ""
            )  # Default to empty string if deprel is not present
            G.add_edge(source, target, label=label)

        # Generate layout and draw the graph
        G.layout(prog="dot")

        # Set filename to default if not provided
        if not filename:
            filename = "graph.png"

        # Draw graph to the specified file
        G.draw(filename)
        print(f"Graph image '{filename}' has been generated.")

        # Display the graph image if we're in a Jupyter notebook environment
        img = mpimg.imread(filename)
        plt.figure(figsize=(10, 10))
        plt.imshow(img)
        plt.axis("off")
        plt.show()

        # Clear the current figure
        plt.clf()

    def is_verb_normal(self, verb):
        """
        verb on "normaalne", kui pole umbisikuline ja verbi aeg on
        past' või 'impf' või 'pres'
        """
        feats = self.nodes[verb]["feats"].keys()
        # kui on umbisikuline
        if "imps" in feats:
            return False

        # tense pole past, impf, pres
        if not len(ListUtils.list_intersection(["past", "impf", "pres"], feats)):
            return False
        return True
