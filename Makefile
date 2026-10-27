.PHONY: setup profile seed-draft run test clean

setup:
	python3 -m venv .venv
	.venv/bin/pip install -q -r requirements.txt

profile:
	.venv/bin/python -m pipeline.profile

seed-draft:
	.venv/bin/python -m pipeline.profile --write-draft

run:
	.venv/bin/python run.py

test:
	.venv/bin/python -m pytest -q

clean:
	rm -rf data output .pytest_cache
