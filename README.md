# Tabelite kirjeldus

## 1. tabel:
Kõik laused, mis sisaldavad mõnda selle tabeli esimesel lehel (DA) olevat lemmat, mille otseste alluvate hulgas on da-infinitiiv (vist märgendiga inf?). Esimeses veerus võiks olla lause, teises veerus otsitava verbi lemma ja kolmandas veerus da-infinitiivis verbi lemma.

Lisaks oleks vaja iga otsitava verbi koguarvu korpuses ja kõigi verbide arvu korpuses (et arvutada assotsiatsioonitugevusi).

##2. tabel:
Kõik laused, mis sisaldavad mõnda sama tabeli kolmandal lehel (MA) olevat lemmat, mille otseste alluvate hulgas on ma-infinitiiv (ainult ma-vorm, mitte mas, mast, mata, maks). Samamoodi esimeses veerus lause, teises veerus otsitava verbi lemma ja kolmandas veerus ma-infinitiivis verbi lemma.

Siin oleks ka vaja iga otsitava verbi kogusagedust korpuses.




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

```bash
python -m ipykernel install --user --name=prg1978_3.12 --display-name=prg1978_3.12
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