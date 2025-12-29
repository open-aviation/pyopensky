# Agent development guide

## Setup commands

- Run `uv sync --dev --extra rs1090 --extra cartes`

## Coding style

- Run `uv run ruff check` and `uv run ruff format` to check and format code
- Run `uvx ty check src/ tests/` to check types

## Documentation commands

- Build documentation: run `uv run make html` from the docs/ folder
- Only capitalize the first letter of the titles and headings, unless there are proper nouns

## Commit rules

- **NEVER COMMIT ANYTHING WITHOUT EXPLICIT APPROVAL**: encourage the developer to check the code before committing
- **DO NOT COMMIT DIRECTLY TO THE MAIN BRANCH**
- Create a feature branch from main for your changes
