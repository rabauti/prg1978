# prg1978

Selles harus otsime lauseid korpusest "Eesti keele kui teise keele kooliõpikute lausete korpus 2021"

Korpus on antud päringute jaoks süntaktiliselt märgendatud (Stanza, estNLTK) ja salvestatud conllu formaadis. Laused pole unikaalsed.

Korpuse DOI: [10.15155/3-00-0000-0000-0000-0888DL](https://metashare.ut.ee/repository/browse/estonian-as-a-second-language-school-coursebook-sentences-corpus-2021/3b2e6438a0d411eebb4773db10791bcffe3bee58c0064a8893e1b696f9ee81d5/)



#  Python env setup


## Creating virtual environment
```bash
python3.12 -m venv venv
```

## Activating venv
```bash
source venv/bin/activate
```


## Installing requirements with pip
```bash
pip install --upgrade pip
pip install -r requirements.txt
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