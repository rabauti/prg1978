# prg1978
scripts for project prg1978



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