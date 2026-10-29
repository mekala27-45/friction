# The build's entry points. Each target is one script; nothing here calls the workspace.
.PHONY: setup test fixture full models grade export monitor manifest docs site gates

LAKE ?= data/lake/full
WORK ?= data/work/models/full

setup:
	uv sync --extra spark

test:
	uv run pytest --cov

fixture:
	uv run python scripts/run_portable.py --landing data/fixtures/landing --out data/lake/fixture
	uv run python scripts/run_features.py --lake data/lake/fixture

full:
	uv run python scripts/run_portable.py --landing data/landing --out $(LAKE)
	uv run python scripts/run_features.py --lake $(LAKE)
	uv run python scripts/summarize_lake.py --lake $(LAKE) --engine local

models:
	uv run python scripts/run_models.py --lake $(LAKE) --out $(WORK)

grade:
	uv run python scripts/grade.py --scored $(WORK)/scored --source local

export:
	uv run python scripts/export_models.py --lake $(LAKE) --work $(WORK)

monitor:
	uv run python scripts/monitor.py --lake $(LAKE) --no-fail

manifest:
	uv run python scripts/build_manifest.py

docs: manifest
	uv run python scripts/check_published_numbers.py --write

site: manifest
	uv run python scripts/build_site_data.py
	cd web && npx next build

gates:
	uv run python scripts/check_no_em_dash.py
	uv run python scripts/check_vocabulary.py
	uv run python scripts/check_statement.py
	uv run python scripts/check_tokens.py
	uv run python scripts/check_registration.py
	uv run python scripts/check_contracts.py
	uv run python scripts/scan_for_planted_identifiers.py
	uv run python scripts/check_published_numbers.py
	node scripts/validate_palette.js --config web/src/theme/palette.json
