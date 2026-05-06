# prg1978

Selles harus asub kood konstruktsioonide otsimiseks Stanzaga süntaktiliselt märgendatud (väikse mahuga) korpusest .

Korpus on antud paringute jaoks syntaktiliselt margendatud ja salvestatud CoNLL-U formaadis. Laused pole unikaalsed.

## Struktuur

```text
notebooks/        notebookid katsetamiseks, statistika kogumiseks ja päringute vaatamiseks
src/data_helpers/ korduskasutatav Python-pakett
data/raw/         allika conllu fail
data/derived/     genereeritud statistika ja päringute tulemused
```

## Python env setup

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv sync --no-install-project
```

## Kohalik seadistus

Vaikimisi seadistused on failis `config.py`.
Masinapohiste failiteede jaoks tee juurkausta oma `config_local.py`, mis on gitignore all.
Naidis on failis `config_local.example.py`.

## Notebookid

Notebookid asuvad kaustas `notebooks/`.

Statistika kogumine:

- `notebooks/010.collect_stats.ipynb`

Paringute ja puude vaatamine:

- `notebooks/110.minema_kohakaanded.ipynb`
- `notebooks/1.da_loend_xcomp.ipynb`
- `notebooks/test.ipynb`

## Graphviz

Pythoni `graphviz` pakett tuleb nüüd kaasa `uv sync` kaudu.
Süsteemi `dot` käsu jaoks on siiski vaja Graphviz eraldi installida.

macOS-is:

```bash
brew install graphviz
```

`SyntaxGraph` toodab nüüd ainult DOT-kuju (`to_dot()`).
Renderdamine käib eraldi abifunktsiooniga `render_syntax_graph()` moodulis `data_helpers.syntax.render_graphviz`, mis kasutab süsteemi `dot` käsku.
Renderdaja ise ei tea projekti kaustadest midagi: `output_dir` antakse talle kutsumise hetkel.
Notebookites suunatakse väljund projekti kausta `tmp/`.
