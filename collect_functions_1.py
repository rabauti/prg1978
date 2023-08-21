import sys
import sqlite3
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
        'verb',
        'verb_compound',
        'deprel1',
        'case1',
        'verbform1',
        'obl_case1',
        'deprel2',
        'case2',
        'verbform2',
        'obl_case2',

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
        self._cursor.execute(f"""CREATE TABLE IF NOT EXISTS collections_processed
                        (tablename text, lastcollection integer);
                        """)

        self._cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS collections_processed_uniq ON collections_processed(tablename);
      """)

        # tsv failist lugemise korral loome tabeli alati nullist
        self._cursor.execute(f"""
          INSERT INTO collections_processed VALUES (?,?)
          ON CONFLICT(tablename) DO UPDATE SET lastcollection=?;""", (self._TABLE1_NAME, 0, 0,))

        self._cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self._TABLE1_NAME}
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
                        );
                       """)

        # add uniq_index on all fields beside id and total
        INDEXNAME = f'{self._TABLE1_NAME}_unique'
        self._cursor.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS {INDEXNAME}
          ON {self._TABLE1_NAME}(
                `verb`
                , `verb_compound` 
                , `deprel1` 
                , `case1` 
                , `verbform1` 
                , `obl_case1` 
                , `deprel2` 
                , `case2` 
                , `verbform2` 
                , `obl_case2` 
              );
          """)

        # tsv failist lugemise korral loome tabeli alati nullist
        self._cursor.execute(f"""DELETE FROM {self._TABLE1_NAME};""")

        if do_truncate:
            self._cursor.execute(f"DELETE FROM {self._TABLE1_NAME} WHERE 1;")

        self._connection.commit()

    def save_coll_to_db(self, collocations, lastcollection):
        sql_colls = []
        sql_examples = []
        for key in collocations.keys():
            # total = some of cases + opposite case

            sql_colls.append((key[0],  # verb
                              key[1],  # verb_compound
                              key[2],  # deprel1
                              key[3],  # case1
                              key[4],  # verbform1
                              key[5],  # obl_case1
                              key[6],  # deprel2
                              key[7],  # case2
                              key[8],  # verbform2
                              key[9],  # obl_case2
                              collocations[key]['total']  # count
                              ))

        self._cursor.executemany(f"""
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
            , count )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                `count` = `count` + excluded.`count`
                ;
        """, sql_colls)

        self._cursor.execute(f"""
          INSERT INTO collections_processed VALUES (?,?)
          ON CONFLICT(tablename) DO UPDATE SET lastcollection=?;""", (
            self._TABLE1_NAME,
            lastcollection,
            lastcollection,
        ))

        self._connection.commit()
        eprint(f'andmebaasi salvestatud kollokatsioonid kollektsioonidest: 0 - {lastcollection}')

    def index_fields(self):
        indexesQ = []
        for field in list(self.key_fields) + ['count']:
            direction = 'ASC' if field not in ['count'] else 'DESC'
            indexesQ.append(f'CREATE INDEX IF NOT EXISTS "`{field}`" ON '
                            f' "{self._TABLE1_NAME}" ("`{field}`" {direction});')
        for q in indexesQ:
            self._cursor.execute(q)


def eprint(*args, **kwargs):
    print(*args, file=sys.stdout, **kwargs)


def extract_something(graph, collection_id, collocations):

    # graph.draw_graph()

    # matrix for node distances
    dpath = graph.get_distances_matrix()

    # ---
    # 2. collect verbs, compounds node ids

    # verb nodes
    verb_nodes = graph.get_nodes_by_attributes(attrname='pos', attrvalue='V')
    # print ('verb_nodes', verb_nodes)

    # compound:prt
    compound_nodes = graph.get_nodes_by_attributes(attrname='deprel', attrvalue='compound:prt')

    # ---
    # 3. collect obls
    obl_nodes = graph.get_nodes_by_attributes(attrname='deprel', attrvalue='obl')

    # iteratsioon üle verbide
    # kogume kokku compound järjestatud
    # itereerime üle obl fraaside


    # key = ( 'verb', 'verb_compound', 'deprel1', 'case1', 'verbform1', 'obl_case1', 'deprel2', 'case2', 'verbform2', 'obl_case2')
    for verb in verb_nodes:

        # do skip collocation if verb is "unusual"
        #if not graph.is_verb_normal(verb):
        #    continue

        # childnodes
        kids = [k for k in dpath[verb] if dpath[verb][k] == 1]
        v_lemma = graph.nodes[verb]['lemma']
        # print(v_lemma)

        # compound children
        n_compounds = ListUtils.list_intersection(kids, compound_nodes)
        if not len(n_compounds):
            verb_compound = ''
            n_compounds.append(None)
        else:
            verb_compound = ', '.join([graph.nodes[n]['lemma'] for n in sorted(n_compounds) if n])

        # if obl_data is empty, there is no need to further check
        if not len(kids):
            # key = ( 'verb', 'verb_compound', 'deprel1', 'case1', 'verbform1', 'obl_case1', 'deprel2', 'case2', 'verbform2', 'obl_case2')
            key = (v_lemma, verb_compound, '', '', '', '', '', '', '', '',)
            collocations = add_key_in_collocations(key, collocations)
            collocations[key]['total'] += 1
            # add to collacations
            continue
        kids_with_required_data = collect_kids_data(graph, kids)

        #print(kids_with_required_data)

        # collect all child dependencies
        for k1 in kids_with_required_data:
            for k2 in kids_with_required_data:
                (deprel1, case1, verbform1, obl_case1) = k1
                (deprel2, case2, verbform2, obl_case2) = k2
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
                collocations[key]['total'] += 1

    return collocations,

def collect_kids_data(graph, nodes_ids):
    dpath = graph.get_distances_matrix()
    spans = []
    for n in nodes_ids:
        deprel = graph.nodes[n]['deprel']
        case = graph.get_node_case(n)
        if deprel in ('xcomp', 'advcl') and graph.nodes[n]['verbform'] in ('ma', 'mast', 'mata', 'vat', 'maks', 'mas', 'da', 'des'):
            verbform = graph.nodes[n]['verbform']
        else:
            verbform = ''

        # obl puhul korjame kokku kõik alluvad kaassõnad

        obl_case_lemmas = []
        if deprel in 'obl':
            obl_kids = [k for k in dpath[n] if dpath[n][k] == 1]
            for kid in obl_kids:
                if (graph.nodes[kid]['deprel'], graph.nodes[kid]['pos'], ) == ('case', 'K', ):
                    obl_case_lemmas.append(graph.nodes[kid]['lemma'])
        obl_case_k = ','.join(sorted(list(set(obl_case_lemmas))))
        span = (deprel, case, verbform, obl_case_k)
        spans.append(span)
    spans = list(set(spans))
    return spans






def add_key_in_collocations(key, collocations):
    if key not in collocations:
        collocations[key] = {'total': 0}
    return collocations
