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
        {"id": "verb", "type": "text"},
        {"id": "verb_person", "type": "text"},
        {"id": "nsubj", "type": "text"},
        {"id": "nsubj_case", "type": "text"},
        {"id": "xcomp", "type": "text"},
        {"id": "verb_is_aux", "type": "int"},
        {"id": "has_obl_ad", "type": "int"},
    ]

    def __init__(self, db_file_name, table1_name, table2_name):
        self._TABLE1_NAME = table1_name
        self._TABLE2_NAME = table2_name
        self._DB_NAME = db_file_name
        self._connection = sqlite3.connect(db_file_name)  #
        self._cursor = self._connection.cursor()

    """
     functions to save script specific data to sqlite data tables
    """

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
                        {key_fields_str},
                        `total` int
                       
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
        # sql_examples = []
        insert_fields_str = ", ".join([f"`{f['id']}`" for f in self.key_fields])
        for key in collocations.keys():
            # total = some of cases + opposite case

            sql_colls.append(
                (
                    key[0],  # verb
                    key[1],  # verb_compound
                    key[2],  # deprel1
                    key[3],  # case1
                    key[4],  # advtype1
                    collocations[key]["total"],
                )
            )

        self._cursor.executemany(
            f"""
        INSERT INTO {self._TABLE1_NAME} (
            {insert_fields_str},
            total
            )

            VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT({insert_fields_str})
            DO UPDATE SET
                `total` = `total` + excluded.`total`
        ;
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
            f"0 - {lastcollection}"
        )

    def index_fields(self):
        indexesQ = []
        for field in list(self.key_fields) + [
            "total",
        ]:
            direction = (
                "ASC"
                if field
                not in [
                    "total",
                ]
                else "DESC"
            )
            indexesQ.append(
                f'CREATE INDEX IF NOT EXISTS "`{field}`" ON '
                f' "{self._TABLE1_NAME}" ("`{field}`" {direction});'
            )
        for q in indexesQ:
            self._cursor.execute(q)

    def dataframe_to_table(dataframe):
        print("implement me")


def eprint(*args, **kwargs):
    print(*args, file=sys.stdout, **kwargs)


def extract_something(graph, collection_id, collocations, verb_global_stat):

    # graph.draw_graph()

    # matrix for node distances
    dpath = graph.get_distances_matrix()

    # ---
    # 2. collect verbs, compounds node ids

    # verb nodes
    verb_nodes = graph.get_nodes_by_attributes(attrname="POS", attrvalue="V")
    # print ('verb_nodes', verb_nodes)

    # TODO, check that nsubj is correct deprel
    nsubj_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="nsubj")

    # xcomp
    xcomp_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="xcomp")

    # ---
    # 3. collect obls
    obl_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="obl")

    # iterate over all verbs

    for verb in verb_nodes:

        # TODO
        # do skip collocation if verb is "unusual"
        # TODO modify conditions
        # aux??
        if do_ignore_verb(verb, graph):
            continue

        # v_deprel = graph.nodes[verb]["deprel"]

        verb_lemma = graph.nodes[verb]["lemma"]
        verb_person = graph.get_node_person(verb)

        # skip verb if verb deprel is 'aux'
        # TODO: add comment why we skip such verbs
        # if v_deprel == "aux":
        #     continue

        # childnodes
        kids = [k for k in dpath[verb] if dpath[verb][k] == 1]

        # skip verb if verb has kid with deprel=aux
        # TODO: add comment why we skip such verbs
        # if "aux" in [graph.nodes[k]["deprel"] for k in kids]:
        #    continue

        for nsubj in nsubj_nodes:
            # kui nsubj pole vahetu alluv, siis ei huvita
            if nsubj not in kids:
                continue

            nsubj_lemma = graph.nodes[nsubj]["lemma"]

            for xcomp in xcomp_nodes:
                if xcomp not in kids:
                    continue
                if graph.nodes[xcomp]["verbform"] not in ["da"]:
                    continue

                xcomp_lemma = graph.nodes[xcomp]["lemma"]

                has_obl_ad = False
                for obl in obl_nodes:
                    if obl not in kids:
                        continue
                    if "ad" not in graph.nodes[obl]["feats"].keys():
                        continue
                    has_obl_ad = True
                    break
                {"id": "verb", "type": "text"},

                key = (
                    verb_lemma,
                    verb_person,
                    nsubj_lemma,
                    nsubj_case,
                    xcomp_lemma,
                    verb_is_aux,
                    has_obl_ad,
                )
                collocations = add_key_in_collocations(key, collocations)
                collocations[key]["total"] += 1
                continue

    return (
        collocations,
        verb_global_stat,
    )


def add_key_in_collocations(key, collocations):
    if key not in collocations:
        collocations[key] = {
            "total": 0,
        }
    return collocations


def do_ignore_verb(verb, graph):
    """
    conditions when verb is ignored
    """

    feats = graph.nodes[verb]["feats"].keys()

    # käskiv
    if "imper" in feats:
        return True

    # kui on umbisikuline
    if "imps" in feats:
        return True

    return False
