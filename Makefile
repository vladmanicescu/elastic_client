# ref: https://venthur.de/2021-03-31-python-makefiles.html
# system python interpreter. used only to create virtual environment
PY = python3
VENV = venv
BIN=$(VENV)/bin



# make it work on windows too
ifeq ($(OS), Windows_NT)
    BIN=$(VENV)/Scripts
    PY=python
endif

$(VENV): requirements.txt
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade -r requirements.txt
	touch $(VENV)
	@echo "virtualenv created."
	@echo "activate venv in current session by running -> . ./$(BIN)/activate "

.PHONY: $(VENV)

all:
	zip -rv ../elastic_client.zip  *

clean:
	rm -rf $(VENV)