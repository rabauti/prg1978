# prg1978

Selles harus asuvad skriptid etteantud konstruktsioonide pärimiseks korpusest "Eesti keele kui teise keele kooliõpikute lausete korpus 2021"

Korpus on antud päringute jaoks süntaktiliselt märgendatud (Stanza, estNLTK) ja salvestatud conllu formaadis. Laused pole unikaalsed.

Korpuse DOI: [10.15155/3-00-0000-0000-0000-0888DL](https://metashare.ut.ee/repository/browse/estonian-as-a-second-language-school-coursebook-sentences-corpus-2021/3b2e6438a0d411eebb4773db10791bcffe3bee58c0064a8893e1b696f9ee81d5/)

# Jupyter notebook

- `000_collect_verbs_stat.ipynb` — korpuse verbide statistika (`lists/stats/verbs.tsv`, `lists/stats/verbs_afiksaaladverb.tsv`).
- `001_collect_lempos_stat.ipynb` — korpuse lemma+POS statistika (`lists/stats/lemmas.tsv`).
- `900.corpus_stat.ipynb` — korpuse tokenite/lausete statistika keeletasemete kaupa (`corp_tokens_stat.csv`).
- `010_make_list_quantifier_1.ipynb`, `011_make_list_quantifier_2.ipynb`, `012_make_list_quantifier_3.ipynb` — otsivad ülemus+alluv (nmod) paare erinevate käändemustritega (kvantori konstruktsioonid); tulemus `lists/results/quantifier_{1,2,3}_*.tsv`.
- `101.da_loend_xcomp.ipynb` — laused, kus nimekirja verbil on da-infinitiivis xcomp-alluv; lisaks filtreeritud kvantori-nimekirja (010-012) järgi.
- `102.ma_loend_xcomp.ipynb` — sama, aga ma-infinitiivis (sup) xcomp-alluv.
- `104.aux_loend_dama.ipynb` — abiverbi (võima/saama/tohtima/pidama) konstruktsioonid; samuti filtreeritud kvantori-nimekirja järgi.

# Python env setup

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv sync --no-install-project
```

## Graphviz

https://graphviz.org/download/

https://stackoverflow.com/questions/69970147/how-do-i-resolve-the-pygraphviz-error-on-mac-os

### MacOS

```
brew --prefix graphviz
```

output:

# /opt/homebrew/opt/graphviz

```
export GRAPHVIZ_DIR="/opt/homebrew/opt/graphviz"
pip install pygraphviz pygraphviz==1.12 \
    --config-settings=--global-option=build_ext \
    --config-settings=--global-option="-I$GRAPHVIZ_DIR/include" \
    --config-settings=--global-option="-L$GRAPHVIZ_DIR/lib"
```
