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

    key_fields = (
        "verb",
        "verb_deprel",
        "verb_compound",
        "deprel1",
        "case1",
        "verbform1",
        "obl_case1",
        "deprel2",
        "case2",
        "verbform2",
        "obl_case2",
    )

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
            """
            CREATE UNIQUE INDEX IF NOT EXISTS collections_processed_uniq
            ON collections_processed(tablename);
            """
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

        self._cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self._TABLE1_NAME}
                        (`id` INTEGER PRIMARY KEY AUTOINCREMENT
                        , `verb` text
                        , `verb_compound` text
                        , `deprel1` text
                        , `case1` text
                        , `verbform1` text
                        , `obl_case1` text
                        , `deprel2` text
                        , `case2` text
                        , `verbform2` text
                        , `obl_case2` text
                        , `count` int
                        , `count_all` int
                        , `deprel1_before` int
                        , `deprel2_before` int
                        , `verb_before` int
                        );
                       """
        )

        # add uniq_index on all fields beside id and total
        INDEXNAME = f"{self._TABLE1_NAME}_unique"
        self._cursor.execute(
            f"""CREATE UNIQUE INDEX IF NOT EXISTS {INDEXNAME}
          ON {self._TABLE1_NAME}(
                `verb`,
                `verb_compound`,
                `deprel1`,
                `case1`,
                `verbform1`,
                `obl_case1`,
                `deprel2`,
                `case2`,
                `verbform2`,
                `obl_case2`
              );
          """
        )

        # tsv failist lugemise korral loome tabeli alati nullist
        self._cursor.execute(f"""DELETE FROM {self._TABLE1_NAME};""")

        if do_truncate:
            self._cursor.execute(f"DELETE FROM {self._TABLE1_NAME} WHERE 1;")

        self._connection.commit()

    def save_coll_to_db(self, collocations, lastcollection):
        sql_colls = []

        for key in collocations.keys():
            # total = some of cases + opposite case

            sql_colls.append(
                (
                    key[0],  # verb
                    key[1],  # verb_compound
                    key[2],  # deprel1
                    key[3],  # case1
                    key[4],  # verbform1
                    key[5],  # obl_case1
                    key[6],  # deprel2
                    key[7],  # case2
                    key[8],  # verbform2
                    key[9],  # obl_case2
                    collocations[key]["total"],  # count
                    collocations[key]["total_all"],  # count
                    collocations[key]["deprel1_before"],  # count
                    collocations[key]["deprel2_before"],  # count
                    collocations[key]["verb_before"],  # count
                )
            )

        self._cursor.executemany(
            f"""
        INSERT INTO {self._TABLE1_NAME} (
            verb
            , verb_compound
            , deprel1
            , case1
            , verbform1
            , obl_case1
            , deprel2
            , case2
            , verbform2
            , obl_case2
            , count
            , count_all
            , deprel1_before
            , deprel2_before
            , verb_before
            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(`verb`
                , `verb_compound`
                , `deprel1`
                , `case1`
                , `verbform1`
                , `obl_case1`
                , `deprel2`
                , `case2`
                , `verbform2`
                , `obl_case2` )
            DO UPDATE SET
                `count` = `count` + excluded.`count`,
                `count_all` = `count_all` + excluded.`count_all`,
                `deprel1_before` = `deprel1_before` + excluded.`deprel1_before`,
                `deprel2_before` = `deprel2_before` + excluded.`deprel2_before`,
                `verb_before` = `verb_before` + excluded.`verb_before`
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
            f" 0 - {lastcollection}"
        )

    def index_fields(self):
        indexesQ = []
        for field in list(self.key_fields) + [
            "count",
            "count_all",
            "deprel1_before",
            "deprel2_before",
            "verb_before",
        ]:
            direction = (
                "ASC"
                if field
                not in [
                    "count",
                    "count_all",
                    "deprel1_before",
                    "deprel2_before",
                    "verb_before",
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

    # key = ( 'verb', 'verb_deprel', 'verb_compound', 'deprel1', 'case1', 'verbform1',
    # 'obl_case1', 'deprel2', 'case2', 'verbform2', 'obl_case2')
    for verb in verb_nodes:

        # do skip collocation if verb is "unusual"
        if do_ignore_verb(verb, graph):
            continue

        v_deprel = graph.nodes[verb]["deprel"]
        # skip verb if verb deprel is 'aux'
        if v_deprel == "aux":
            continue

        # childnodes
        kids = [k for k in dpath[verb] if dpath[verb][k] == 1]

        # jätame vahele kui verbil on alluv aux
        if "aux" in [graph.nodes[k]["deprel"] for k in kids]:
            continue

        v_lemma = graph.nodes[verb]["lemma"]

        # print(v_lemma)

        # compound children
        n_compounds = ListUtils.list_intersection(kids, compound_nodes)
        if not len(n_compounds):
            verb_compound = ""
            n_compounds.append(None)
        else:
            verb_compound = ", ".join(
                [graph.nodes[n]["lemma"] for n in sorted(n_compounds) if n]
            )

        kids_with_required_data_dict = collect_kids_data(graph, kids, no_subj_obj_case)
        kids_with_required_data = list(kids_with_required_data_dict.keys())

        verb_global_key = (
            v_lemma,
            verb_compound,
        )
        verb_global_stat = add_key_in_verb_total_stat(verb_global_key, verb_global_stat)
        verb_global_stat[verb_global_key]["occurences"] += 1

        # if obl_data is empty, there is no need to further check
        if not len(kids) or not len(kids_with_required_data):
            # verbi statistika
            verb_global_stat[verb_global_key]["kids_total"].append(0)
            verb_global_stat[verb_global_key]["kids_uniq_total"].append(0)

            # key = ( 'verb',  'verb_compound', 'deprel1', 'case1',
            # 'verbform1', 'obl_case1', 'deprel2', 'case2', 'verbform2', 'obl_case2')
            key = (
                v_lemma,
                verb_compound,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )
            collocations = add_key_in_collocations(key, collocations)
            collocations[key]["total"] += 1
            collocations[key]["total_all"] += 1
            collocations[key]["deprel1_before"] = 0
            collocations[key]["deprel2_before"] = 0
            collocations[key]["verb_before"] = 0
            # add to collacations
            continue

        # collect all child dependencies
        verb_stat_kids = []

        for k1 in kids_with_required_data:

            for m in range(kids_with_required_data_dict[k1]["total"]):
                verb_stat_kids.append(k1)

            for k2 in kids_with_required_data:

                (deprel1, case1, verbform1, obl_case1) = k1
                (deprel2, case2, verbform2, obl_case2) = k2

                left = []  # deprel1 if first
                right = []  # deprel2 is first
                verb_before = 0
                for k1_node_pos in kids_with_required_data_dict[k1]["position"]:
                    if k1_node_pos > verb:
                        verb_before += 1
                    for k2_node_pos in kids_with_required_data_dict[k2]["position"]:
                        if k1_node_pos == k2_node_pos:
                            continue
                        if k1_node_pos < k2_node_pos:
                            left.append(k1_node_pos)
                        if k1_node_pos > k2_node_pos:
                            right.append(k1_node_pos)

                if (
                    k1 == k2
                    and kids_with_required_data_dict[k1]["total"] == 1
                    and len(kids_with_required_data_dict) == 1
                ):

                    key = (
                        v_lemma,
                        verb_compound,
                        deprel1,
                        case1,
                        verbform1,
                        obl_case1,
                        "",
                        "",
                        "",
                        "",
                    )
                    collocations = add_key_in_collocations(key, collocations)

                else:

                    key = (
                        v_lemma,
                        verb_compound,
                        deprel1,
                        case1,
                        verbform1,
                        obl_case1,
                        deprel2,
                        case2,
                        verbform2,
                        obl_case2,
                    )
                    collocations = add_key_in_collocations(key, collocations)
                # erinevate võtmete (k1 ja k2) puhul loetakse kokku mitu paari saab moodustada
                total_all = (
                    kids_with_required_data_dict[k1]["total"]
                    * kids_with_required_data_dict[k2]["total"]
                )
                # korduvate k1 ja k2 korral loetakse kokku nende tippude lihtesinemiste arv (summa), mitte
                # unikaalsete paaride arv
                # kui on 3 obl gen tippu , siis on total_all 3

                if k1 == k2 and kids_with_required_data_dict[k1]["total"] > 1:
                    n = kids_with_required_data_dict[k1]["total"]
                    total_all = kids_with_required_data_dict[k1]["total"]

                collocations[key]["total"] += 1

                # key = ( 'verb', 'verb_deprel', 'verb_compound', 'deprel1', 'case1',
                # 'verbform1', 'obl_case1', 'deprel2',
                # 'case2', 'verbform2', 'obl_case2')

                collocations[key]["total_all"] += total_all
                collocations[key]["deprel1_before"] += len(left)
                collocations[key]["deprel2_before"] += len(right)
                collocations[key]["verb_before"] += verb_before

        # unikaalsed (filtreeritud) lapsed
        verb_global_stat[verb_global_key]["kids_uniq_total"].append(
            len(list(set(verb_stat_kids)))
        )

        # koik (filtreeritud) lapsed
        verb_global_stat[verb_global_key]["kids_total"].append(len(verb_stat_kids))

    return (
        collocations,
        verb_global_stat,
    )


def collect_kids_data(graph, nodes_ids, no_subj_obj_case=False):

    spans_dict = {}
    dpath = graph.get_distances_matrix()
    # spans = []
    for n in nodes_ids:
        deprel = graph.nodes[n]["deprel"]

        if deprel not in (
            "nsubj",
            "obj",
            "obl",
            "xcomp",
            "advcl",
            "advmod",
            "root",
            "ccomp",
        ):
            continue

        case = graph.get_node_case(n)

        # objektil ja subjektil ei arvesta käänet
        # obj nom, gen ja par eri sageduste asemel on ainult üks suur objekti sagedus
        if no_subj_obj_case and deprel in ("nsubj", "obj"):
            case = "*"

        if deprel in ("xcomp", "advcl") and graph.nodes[n]["verbform"] in (
            "ma",
            "mast",
            "mata",
            "vat",
            "maks",
            "mas",
            "da",
            "des",
        ):
            verbform = graph.nodes[n]["verbform"]
        else:
            verbform = ""
        # advcl ainult siis kui on mõni vorm olemas

        if deprel == "advcl" and verbform == "":
            continue

        # obl puhul korjame kokku kõik alluvad kaassõnad

        obl_case_lemmas = []
        if deprel in "obl":
            obl_kids = [k for k in dpath[n] if dpath[n][k] == 1]
            for kid in obl_kids:
                if (
                    graph.nodes[kid]["deprel"],
                    graph.nodes[kid]["POS"],
                ) == (
                    "case",
                    "K",
                ):
                    obl_case_lemmas.append(graph.nodes[kid]["lemma"])
        obl_case_k = ",".join(sorted(list(set(obl_case_lemmas))))
        span = (deprel, case, verbform, obl_case_k)

        if span in spans_dict:
            spans_dict[span]["total"] += 1
            spans_dict[span]["position"].append(n)
        else:
            spans_dict[span] = {"total": 1, "position": [n]}

    return spans_dict


def add_key_in_collocations(key, collocations):
    if key not in collocations:
        collocations[key] = {
            "total": 0,
            "total_all": 0,
            "deprel1_before": 0,
            "deprel2_before": 0,
            "verb_before": 0,
        }
    return collocations


def add_key_in_verb_total_stat(key, verb_total_stat):
    if key not in verb_total_stat:
        verb_total_stat[key] = {
            "occurences": 0,
            "kids_total": [],
            "kids_uniq_total": [],
        }
    return verb_total_stat


def do_ignore_verb(verb, graph):
    """
    conditions when verb is ignored
    """

    # kõik need vormid
    if graph.nodes[verb]["verbform"] in (
        "ma",
        "mast",
        "mata",
        "v",
        "maks",
        "mas",
        "da",
        "des",
    ):
        return True

    feats = graph.nodes[verb]["feats"].keys()

    # käskiv
    if "imper" in feats:
        return True

    # kui on umbisikuline
    if "imps" in feats:
        return True

    return False


def verb_stat_to_df(verbs_global_stat, filename):
    data = []
    # verb_total_stat[key] = {'occurences': 0, 'kids_total': [], 'kids_uniq_total': []}
    for (
        lemma,
        compound,
    ) in verbs_global_stat:

        arr_kids = verbs_global_stat[
            (
                lemma,
                compound,
            )
        ]["kids_total"]
        arr_uniq_kids = verbs_global_stat[
            (
                lemma,
                compound,
            )
        ]["kids_uniq_total"]
        occurences = verbs_global_stat[
            (
                lemma,
                compound,
            )
        ]["occurences"]
        mean_kids = np.mean(arr_kids)
        mean_kids_uniq = np.mean(arr_uniq_kids)
        median_kids = np.median(arr_kids)
        median_kids_uniq = np.median(arr_uniq_kids)
        min_kids = np.min(arr_kids)
        min_kids_uniq = np.min(arr_uniq_kids)
        max_kids = np.max(arr_kids)
        max_kids_uniq = np.max(arr_uniq_kids)
        pstdev_kids = st.pstdev(arr_kids) if len(arr_kids) > 1 else None
        pstdev_kids_uniq = st.pstdev(arr_uniq_kids) if len(arr_uniq_kids) > 1 else None

        stdev_kids = st.stdev(arr_kids) if len(arr_kids) > 1 else None
        stdev_kids_uniq = st.stdev(arr_uniq_kids) if len(arr_uniq_kids) > 1 else None

        # mode_kids = "%s" % st.mode(arr_kids)
        # mode_kids_uniq = "%s" % st.mode(arr_uniq_kids)
        data.append(
            (
                lemma,
                compound,
                occurences,
                mean_kids,
                mean_kids_uniq,
                median_kids,
                median_kids_uniq,
                min_kids,
                min_kids_uniq,
                max_kids,
                max_kids_uniq,
                stdev_kids,
                stdev_kids_uniq,
                pstdev_kids,
                pstdev_kids_uniq,
            )
        )
    df = pd.DataFrame(
        data,
        columns=[
            "lemma",
            "compound",
            "occurences",
            "mean_kids",
            "mean_kids_uniq",
            "median_kids",
            "median_kids_uniq",
            "min_kids",
            "min_kids_uniq",
            "max_kids",
            "max_kids_uniq",
            "stdev_kids",
            "stdev_kids_uniq",
            "pstdev_kids",
            "pstdev_kids_uniq",
        ],
    )

    df = df.sort_values(
        ["occurences", "lemma", "compound"], ascending=[False, True, True]
    )
    for col in df.columns[2:]:
        df[col] = df[col].round(2)
    df.to_csv(filename, index=None)

    # df.to_sql(name='verbs_global_stat', con=engine)

    print("Verb stat saved to ", filename)

    return df
