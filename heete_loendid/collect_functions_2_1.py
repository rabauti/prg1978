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
        {"id": "csubj", "type": "text"},
        {"id": "has_obl_ad_all", "type": "text"},
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

        # loome näidete faili
        self._cursor.execute(
            f"""CREATE TABLE {self._TABLE2_NAME}
            (row_id integer
            , sentence_id integer
            , verb text
            , sentence_part text
            , sentence_text text
            , verb_id integer);
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
        sql_examples = []
        insert_fields_str = ", ".join([f"`{f['id']}`" for f in self.key_fields])

        for key in collocations.keys():
            # total = some of cases + opposite case
            sql_colls.append(
                (
                    key[0],  # verb
                    key[1],  # csubj
                    key[2],  # has_obl_ad_all
                    collocations[key]["total"],
                )
            )

            for example in collocations[key]["examples"]:
                sql_examples.append(
                    (
                        key[0],  # verb
                        key[1],  # csubj
                        key[2],  # has_obl_ad_all
                        example[0],  # sentence_id
                        example[1],  # verb
                        example[2],  # verb_id
                        example[3],  # sentence_part
                        example[4],  # sentence_text
                    )
                )

        self._cursor.executemany(
            f"""
        INSERT INTO {self._TABLE1_NAME} (
            {insert_fields_str},
            total
            )

            VALUES (?, ?, ?, ?)
        ON CONFLICT({insert_fields_str})
            DO UPDATE SET
                `total` = `total` + excluded.`total`
        ;
        """,
            sql_colls,
        )

        example_where_str = " AND ".join([f"`{f['id']}` = ? " for f in self.key_fields])
        # print(example_where_str)
        self._cursor.executemany(
            f"""INSERT INTO {self._TABLE2_NAME} (
            `row_id`,
            `sentence_id`,
            `verb`,
            `verb_id`,
            `sentence_part`,
            `sentence_text`
            )
            VALUES (
                (SELECT `id` FROM {self._TABLE1_NAME} WHERE {example_where_str}),
                ?, ?, ?, ?, ?);""",
            sql_examples,
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

    verb_nodes = graph.get_nodes_by_attributes(attrname="POS", attrvalue="V")
    csubj_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="csubj")
    obl_nodes = graph.get_nodes_by_attributes(attrname="deprel", attrvalue="obl")

    # iterate over all verbs

    for verb in verb_nodes:
        # do skip collocation if verb is "unusual"
        if do_ignore_verb(verb, graph):
            continue

        verb_lemma = graph.nodes[verb]["lemma"]
        verb_person = graph.get_node_person(verb)
        if verb_person != "ps3":
            continue

        # childnodes
        kids = [k for k in dpath[verb] if dpath[verb][k] == 1]

        has_obl_ad_all = 0
        for obl in obl_nodes:
            if obl not in kids:
                continue
            if "ad" in graph.nodes[obl]["feats"].keys():
                has_obl_ad_all = 1
                continue
            if "all" in graph.nodes[obl]["feats"].keys():
                has_obl_ad_all = 1
                continue
            break

        for csubj in csubj_nodes:
            # kui pole vahetu alluv
            if csubj not in kids:
                continue
            if "inf" not in graph.nodes[csubj]["feats"].keys():
                continue

            csubj_lemma = graph.nodes[csubj]["lemma"]

            key = (
                verb_lemma,
                csubj_lemma,
                has_obl_ad_all,
            )
            collocations = add_key_in_collocations(key, collocations)
            collocations[key]["total"] += 1
            collocations[key]["examples"].append(
                (
                    collection_id,
                    verb_lemma,
                    verb,
                    " ".join([graph.nodes[n]["form"] for n in sorted([verb] + kids)]),
                    " ".join(
                        [graph.nodes[n]["form"] for n in sorted(graph.nodes) if n]
                    ),
                )
            )
            continue

    return (
        collocations,
        verb_global_stat,
    )


def add_key_in_collocations(key, collocations):
    if key not in collocations:
        collocations[key] = {
            "total": 0,
            "examples": [],
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
