import sys
import sqlite3


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
        {"id": "child_lemma", "type": "text"},
        {"id": "child_pos", "type": "text"},
        {"id": "child_case", "type": "text"},
        {"id": "child_number", "type": "text"},
        {"id": "parent_lemma", "type": "text"},
        {"id": "parent_pos", "type": "text"},
        {"id": "parent_case", "type": "text"},
        {"id": "parent_number", "type": "text"},
        {"id": "text", "type": "text"},
        {"id": "sentence", "type": "text"},
        {"id": "sentence_id", "type": "int"},
        {"id": "child_lemma_total", "type": "int"},
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
        # INDEXNAME = f"{self._TABLE1_NAME}_unique"

        # antud juhul me ei koonda ridu
        # index_fields_str = ", ".join([f"`{f['id']}`" for f in self.key_fields])
        # self._cursor.execute(
        #    f"""CREATE UNIQUE INDEX IF NOT EXISTS {INDEXNAME}
        #  ON {self._TABLE1_NAME}({index_fields_str});
        #  """
        # )

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
                    key[0],  #
                    key[1],  #
                    key[2],  #
                    key[3],  #
                    key[4],  #
                    key[5],  #
                    key[6],  #
                    key[7],  #
                    key[8],  #
                    key[9],  #
                    key[10],  #
                    key[11],  #
                )
            )

        self._cursor.executemany(
            f"""
        INSERT INTO {self._TABLE1_NAME} (
            {insert_fields_str}
            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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


def extract_something(graph, collection_id, collocations, lemmas_stat):
    sentence_id = collection_id

    # graph.draw_graph()

    # matrix for node distances
    dpath = graph.get_distances_matrix()

    # ---
    # 2. collect S nodes

    # noun nodes
    noun_nodes = graph.get_nodes_by_attributes(attrname="POS", attrvalue="S")
    # print ('verb_nodes', verb_nodes)

    # nmod
    nmod_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="nmod")

    # iteratsioon üle nimisõnade

    # higlight_nodes = []
    # draw_tree = False
    # key = ( 'verb', 'verb_compound', 'verb_feats',
    # 'inf_verb', 'inf_verb_feats', 'sentence', 'sentence_id',)
    for noun in noun_nodes:

        # {"id": "child_lemma", "type": "text"},
        # {"id": "child_pos", "type": "text"},
        # {"id": "child_case", "type": "text"},
        # {"id": "child_number", "type": "text"},
        # {"id": "parent_lemma", "type": "text"},
        # {"id": "parent_pos", "type": "text"},
        # {"id": "parent_case", "type": "text"},
        # {"id": "parent_number", "type": "text"},
        # {"id": "text", "type": "text"},
        # {"id": "sentence", "type": "text"},
        # {"id": "sentence_id", "type": "int"},
        # {"id": "child_lemma_total", "type": "int"},

        noun_lemma = graph.nodes[noun]["lemma"]
        noun_pos = graph.nodes[noun]["POS"]
        noun_case = graph.get_node_case(noun)
        noun_number = graph.get_node_number(noun)

        if noun_case not in ["part"]:
            continue
        # childnodes
        kids = [k for k in dpath[noun] if dpath[noun][k] == 1]

        # iterate over nmod children
        for nmod in nmod_nodes:
            # kui pole vahetu alluv, siis ei huvita
            if nmod not in kids:
                continue

            nmod_case = graph.get_node_case(nmod)
            nmod_pos = graph.nodes[nmod]["POS"]
            if nmod_case not in ["nom", "gen", "part"]:
                continue
            if nmod_pos != "S":
                continue

            nmod_lemma = graph.nodes[nmod]["lemma"]

            nmod_number = graph.get_node_number(nmod)
            # graph.draw_graph(
            #    highlight=[noun, nmod],
            # )

            text = ""
            words = []
            for n in sorted(graph.nodes):
                if not n:
                    continue
                if n in (
                    noun,
                    nmod,
                ):
                    words.append(f'___{graph.nodes[n]["form"]}___')
                else:
                    words.append(graph.nodes[n]["form"])
            sentence_text = " ".join(words)

            text = " ".join(
                [
                    graph.nodes[n]["form"]
                    for n in sorted(
                        (
                            noun,
                            nmod,
                        )
                    )
                ]
            )

            # lisame andmed baasi
            key = (
                nmod_lemma,  # child_lemma
                nmod_pos,  # child_lemma
                nmod_case,  # child_case
                nmod_number,  # child_number
                noun_lemma,  # parent_lemma
                noun_pos,  # parent_lemma
                noun_case,  # parent_case
                noun_number,  # parent_number
                text,  # text
                sentence_text,  # sentence
                sentence_id,  # sentence_id
                lemmas_stat[
                    "%s\t%s"
                    % (
                        nmod_lemma,
                        nmod_pos,
                    )
                ],  # nmod_lemma_total_occurences,  # child_lemma_total_occurences
            )

            collocations.append(key)

    return collocations
