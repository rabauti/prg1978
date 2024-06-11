import sys
import sqlite3
import numpy as np
import statistics as st
import pandas as pd
from data_helpers.utils import ListUtils


class DbMethods:
    """
    Class for creating and storing data in sqlite database
    """

    _cursor = None
    _connection = None

    _DB_NAME = None
    _TABLE1_NAME = None
    _TABLE2_NAME = None

                    
    key_fields = [
        {"id": "verb", "type": "text"},
        {"id": "verb_compound", "type": "text"},
        {"id": "verb_feats", "type": "text"},
        {"id": "verb_is_aux", "type": "int"},
        {"id": "verb_deprel", "type": "text"},
        {"id": "sup_verb", "type": "text"},
        {"id": "sup_verb_feats", "type": "text"},
        {"id": "sup_verb_deprel", "type": "text"},
        {"id": "sentence", "type": "text"},
        {"id": "sentence_id", "type": "text"},
    ]

    def __init__(self, db_file_name, table1_name, table2_name):
        self._TABLE1_NAME = table1_name
        self._TABLE2_NAME = table2_name
        self._DB_NAME = db_file_name
        self._connection = sqlite3.connect(db_file_name)  #
        self._cursor = self._connection.cursor()

    def prep_coll_db(self, do_truncate=True):
        self._cursor.execute(
            """CREATE TABLE IF NOT EXISTS collections_processed
            (tablename text, lastcollection integer);
            """
        )

        self._cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS"
            " collections_processed_uniq ON collections_processed(tablename);"
        )

        # tsv failist lugemise korral loome tabeli alati nullist
        self._cursor.execute(
            """
          INSERT INTO collections_processed VALUES (?,?)
          ON CONFLICT(tablename) DO UPDATE SET lastcollection=?;""",
            (
                self._TABLE1_NAME,
                0,
                0,
            ),
        )

        key_fields_str = ", ".join(
            [f"`{f['id']}` {f['type']} " for f in self.key_fields]
        )
        self._cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self._TABLE1_NAME}
                        (`id` INTEGER PRIMARY KEY AUTOINCREMENT,
                        {key_fields_str}
                        );
             """
        )


        # add uniq_index on all fields beside id and total
        INDEXNAME = f"{self._TABLE1_NAME}_unique"

        index_fields_str = ", ".join([f"`{f['id']}`" for f in self.key_fields])
        self._cursor.execute(
            f"""CREATE UNIQUE INDEX IF NOT EXISTS {INDEXNAME}
          ON {self._TABLE1_NAME}({index_fields_str});
          """
        )

        # tsv failist lugemise korral loome tabeli alati nullist
        self._cursor.execute(f"""DELETE FROM {self._TABLE1_NAME};""")

        if do_truncate:
            self._cursor.execute(f"DELETE FROM {self._TABLE1_NAME} WHERE 1;")

        self._connection.commit()

    def save_coll_to_db(self, collocations, lastcollection):
        sql_colls = []
        insert_fields_str = ", ".join([f"`{f['id']}`" for f in self.key_fields])
        for key in collocations:
            # total = some of cases + opposite case
            sql_colls.append(
                (
                    key[0],  # verb
                    key[1],  # verb_compound
                    key[2],  # verb_feats
                    key[3],  # verb_is_aux
                    key[4],  # verb_deprel
                    key[5],  # sup_verb
                    key[6],  # sup_verb_feats
                    key[7],  # sup_verb_deprel
                    key[8],  # sentence
                    key[9],  # sentence_id
                )
            )

        self._cursor.executemany(
            f"""
        INSERT INTO {self._TABLE1_NAME} (
            {insert_fields_str}
            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            sql_colls,
        )


        self._cursor.execute(
            """
          INSERT INTO collections_processed VALUES (?,?)
          ON CONFLICT(tablename) DO UPDATE SET lastcollection=?;""",
            (
                self._TABLE1_NAME,
                lastcollection,
                lastcollection,
            ),
        )

        self._connection.commit()
        eprint(
            "andmebaasi salvestatud kollokatsioonid kollektsioonidest:"
            f" 0 - {lastcollection}"
        )

    def dataframe_to_table(dataframe):
        print("implement me")


def eprint(*args, **kwargs):
    print(*args, file=sys.stdout, **kwargs)


def extract_something(
    graph, collection_id, collocations, verb_global_stat, no_subj_obj_case=False
):

    # graph.draw_graph()

    # matrix for node distances
    dpath = graph.get_distances_matrix()

    # ---
    # 2. collect verbs, compounds node ids

    # verb nodes
    verb_nodes = graph.get_nodes_by_attributes(attrname="POS", attrvalue="V")
    # print ('verb_nodes', verb_nodes)

    # compound:prt
    compound_nodes = graph.get_nodes_by_attributes(
        attrname="deprel", attrvalue="compound:prt"
    )

    # iteratsioon üle verbide
    # kogume kokku compound järjestatud

    higlight_nodes = []
    draw_tree = False
    # key = ( 'verb', 'verb_compound', 'verb_feats',
    # 'sup_verb', 'sup_verb_feats', 'sentence', 'sentence_id',)
    for verb in verb_nodes:

        verb_lemma = graph.nodes[verb]["lemma"]

        verb_deprel = graph.nodes[verb]["deprel"]
        verb_feats = ",".join(graph.nodes[verb]["feats"])

        verb_is_aux = 0

        # verb_is_aux  verb's deprel==aux or verb has kid with deprel == aux

        if graph.nodes[verb]["deprel"] == "aux":
            verb_is_aux = 1

        # childnodes
        kids = [k for k in dpath[verb] if dpath[verb][k] == 1]

        # jätame vahele kui verbil on alluv aux
        if "aux" in [graph.nodes[k]["deprel"] for k in kids]:
            verb_is_aux = 1

        sentence_id = collection_id

        # compound children
        n_compounds = ListUtils.list_intersection(kids, compound_nodes)
        if not len(n_compounds):
            verb_compound = ""
            n_compounds.append(None)
        else:
            verb_compound = ", ".join(
                [graph.nodes[n]["lemma"] for n in sorted(n_compounds) if n]
            )

        # iterate over verb sup verb children
        for ma_verb in verb_nodes:
            # kui pole vahetu alluv, siis ei huvita
            if ma_verb not in kids:
                continue

            # kui pole da-infinitiiv, siis ei huvita
            if "sup" not in graph.nodes[ma_verb]["feats"].keys():
                continue
            
            # kui vorm pole ma, siis ei huvita
            if not "ma" == graph.nodes[ma_verb]["verbform"]:
                continue
            
            sup_verb = graph.nodes[ma_verb]["lemma"]
            sup_verb_feats = ",".join(graph.nodes[ma_verb]["feats"])
            sup_verb_deprel = graph.nodes[ma_verb]["deprel"]

            words = []
            for n in sorted(graph.nodes):
                if not n:
                    continue
                if n in (verb, ma_verb):
                    words.append(f'___{graph.nodes[n]["form"]}___')
                else:
                    words.append(graph.nodes[n]["form"])
            sentence_text = " ".join(words)
            # lisame andmed baasi
            key = (
                verb_lemma,  # verb
                verb_compound,  # verb_compound
                verb_feats,  # verb_feats
                verb_is_aux,  # verb_is_aux
                verb_deprel,  # verb_deprel
                sup_verb,  # sup_verb
                sup_verb_feats,  # sup_verb_feats
                sup_verb_deprel,  # sup_verb_deprel
                sentence_text,  # sentence
                sentence_id,  # sentence_id
            )

            collocations.append(key)
            if verb_lemma in ["kibelema", "plaanitsema"]:
                graph.draw_graph2(
                    filename=f"img_ma/{collection_id}_{verb_lemma}_{verb_compound}_{sup_verb}.png",
                    highlight=[verb, ma_verb],
                )

    return (
        collocations,
        {},
    )

