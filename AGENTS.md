# Repository Guidelines

## Project Structure & Module Organization
- src/: Python source code using a src layout.
  - src/f1_podium/: Core package (flows, DB connector).
    - postgresql_conn.py: Prefect Block for SQLAlchemy engine.
    - race_predictions.py: Prefect flow to load data and run predictions.
    - run_schedueler.py: Prefect flow to schedule upcoming runs with FastF1.
- scripts/: Reserved for helper utilities (optional).
- .env: Local environment values (never commit secrets).
- .venv/: Local virtual environment (ignored).

## Build, Test, and Development Commands
- Create venv: `python -m venv .venv && source .venv/bin/activate`
- Install deps (example): `pip install pandas prefect sqlalchemy fastf1 requests`
- Run scheduler flow: `python src/f1_podium/run_schedueler.py`
- Run predictions flow: `python src/f1_podium/race_predictions.py`
- Optional module style: `PYTHONPATH=src python -m f1_podium.run_schedueler`

## Coding Style & Naming Conventions
- Python 3.12+, PEP 8, 4‑space indentation.
- Names: snake_case for modules/functions, PascalCase for classes, UPPER_CASE for constants.
- Type hints where practical; keep functions small and testable.
- Suggested tools: `ruff` for linting (`ruff check src`) and `black` for formatting (`black src`).

## Testing Guidelines
- Framework: pytest. Place tests under `tests/` with files like `test_<module>.py`.
- Run: `pytest -q` (aim for coverage on core data transforms and DB I/O boundaries).
- Prefer small, deterministic tests; mock external APIs (requests, database engine).

## Commit & Pull Request Guidelines
- Commits: use Conventional Commits where possible, e.g. `feat: add schedule flow`, `fix: handle missing fastest lap`.
- PRs: include summary, rationale, screenshots/logs if helpful, run commands to verify, and linked issues.
- Keep PRs focused; add migration or config notes if DB schema/blocks change.

## Security & Configuration Tips
- Prefect Block: create a block named `postgresdb` before running flows. Example:
  `python -c "from src.f1_podium.postgresql_conn import PostgresqlConnector as C; C(user='u', db_name='db', host='h', password='p', port=5432).save('postgresdb', overwrite=True)"`
- Do not commit secrets, databases, or large artifacts; `.env`, `*.db` are ignored by `.gitignore`.
- Networked calls (Ergast API, FastF1) may fail; handle retries and timeouts where added.
