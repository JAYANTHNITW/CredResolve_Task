.PHONY: test sim load demo

test:
	python -m pytest -q

sim:
	python -m smartdialer.simulator

load:
	python scripts/loadtest.py

demo:
	python -m pytest -s tests/scenarios

.PHONY: ui
ui:
	python -m smartdialer.ui.server
