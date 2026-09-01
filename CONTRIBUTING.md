# Contributing to Text2SQL Evaluation Toolkit

Thank you for your interest in contributing to the Text2SQL Evaluation Toolkit! This document provides guidelines for contributing to the project.
Our project welcomes external contributions. If you have an itch, please feel free to scratch it.

To contribute code or documentation, please submit a [pull request](https://github.com/IBM/text2sql-eval-toolkit/pulls).

A good way to familiarize yourself with the codebase and contribution process is to look for and tackle low-hanging fruit in the [issue tracker](https://github.com/IBM/text2sql-eval-toolkit/issues).
Before embarking on a more ambitious contribution, please quickly [get in touch](#communication) with us.

**Note: We appreciate your effort, and want to avoid a situation where a contribution requires extensive rework (by you or by us), sits in backlog for a long time, or cannot be accepted at all!**

### Proposing new features

If you would like to implement a new feature, please [raise an issue](https://github.com/IBM/text2sql-eval-toolkit/issues) before sending a pull request so the feature can be discussed. This is to avoid you wasting your valuable time working on a feature that the project developers are not interested in accepting into the code base.

### Fixing bugs

If you would like to fix a bug, please [raise an issue](https://github.com/IBM/text2sql-eval-toolkit/issues) before sending a pull request so it can be tracked.

### Merge approval

The project maintainers use LGTM (Looks Good To Me) in comments on the code review to indicate acceptance. A change requires LGTMs from two of the maintainers of each component affected.

For a list of the maintainers, see the [MAINTAINERS.md](MAINTAINERS.md) page.


## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.


## Legal

Each source file must include a license header for the Apache Software License 2.0. Using the SPDX format is the simplest approach.
e.g.

```
#
# Copyright IBM Corp. 2025 - 2025
# SPDX-License-Identifier: Apache-2.0
#
```

We have tried to make it as easy as possible to make contributions. This applies to how we handle the legal aspects of contribution. We use the same approach - the [Developer's Certificate of Origin 1.1 (DCO)](https://github.com/hyperledger/fabric/blob/master/docs/source/DCO1.1.txt) - that the Linux® Kernel [community](https://elinux.org/Developer_Certificate_Of_Origin) uses to manage code contributions.

We simply ask that when submitting a patch for review, the developer must include a sign-off statement in the commit message.

Here is an example Signed-off-by line, which indicates that the submitter accepts the DCO:

```
Signed-off-by: John Doe <john.doe@example.com>
```

You can include this automatically when you commit a change to your local git repository using the following command:

```
git commit -s
```

## Communication

Please feel free to connect with us by opening an issue in the [issue tracker](https://github.com/IBM/text2sql-eval-toolkit/issues).

## Development Setup

### Prerequisites

- Python 3.11 or higher
- Git

### Setting Up Your Development Environment

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/text2sql-eval-toolkit.git
cd text2sql-eval-toolkit

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with the dev toolchain
pip install -e ".[dev,dashboard]"

# Add database extras only if you run those benchmarks locally
pip install -e ".[dev,dashboard,db2,mysql,presto]"
```

All tool configuration lives in `pyproject.toml`, so the versions you run locally
match the versions CI enforces.

### Running Tests

```bash
# Default suite: hermetic -- no network, no credentials, no databases
pytest

# With coverage
pytest --cov

# A specific file
pytest tests/test_text2sql_metrics.py
```

Tests that need live LLM or database credentials are marked `integration` and are
excluded from the default run. Run them deliberately:

```bash
# Requires the credentials in env.example to be configured
pytest -m integration
```

Hub network tests additionally require `RUN_NETWORK_TESTS=1`.

## The published surface is a contract

Everything exported from `text2sql_eval_toolkit.__init__` is on PyPI and is
imported by people who are not in this repository. **It does not change because
the dashboard needs something.** No signature, no return shape, no default.

Dashboard features live in `ui/`. Where one genuinely needs something from the
library, the library gains an *optional* parameter that defaults to today's
behaviour — the per-user API keys in 1.4.0 are the worked example: the clients
took an optional `api_key`, and a call that passes nothing reads the environment
exactly as it always did.

This is enforced rather than trusted. `tests/test_public_api.py` fails when an
exported symbol disappears, loses its docstring, or documents an argument it does
not take, and the characterisation tests fail when behaviour changes. A
deliberate change to the public surface therefore has to change a test too, which
is the point: it makes the decision visible in review instead of shipping as a
side effect.

## Coding style guidelines

These are enforced by CI on every pull request, so run them before pushing:

- **Formatting**: [Black](https://black.readthedocs.io/), line length 88
  ```bash
  black src/ tests/ scripts/
  ```

- **Linting**: [Ruff](https://docs.astral.sh/ruff/)
  ```bash
  ruff check src/ tests/ scripts/
  ```

- **Type checking**: [mypy](https://mypy.readthedocs.io/) over the modules listed
  under `[tool.mypy]` in `pyproject.toml`. The scope is intentionally narrow and
  widens as annotations land.
  ```bash
  mypy
  ```

- **Dashboard**: lint and build must pass, and the bundle stays within the size
  budget checked in CI.
  ```bash
  cd dashboard && npm ci && npm run lint && npm run build
  ```

- **Type Hints**: Add type hints to function signatures
- **Docstrings**: Use Google-style docstrings for functions and classes
- **Line Length**: Maximum 88 characters (Black default)

## Project Structure

```
text2sql-eval-toolkit/
├── src/text2sql_eval_toolkit/  # Main package
│   ├── evaluation/             # Evaluation metrics
│   ├── execution/              # SQL execution
│   ├── inference/              # LLM inference
│   ├── profiling/              # Query profiling
│   └── analysis/               # Results analysis
├── scripts/                    # Utility scripts
├── data/                       # Benchmarks and results
├── tests/                      # Test suite
└── notebooks/                  # Example notebooks
```

## Adding New Features

### Adding a New Benchmark

1. Create benchmark JSON file in `data/benchmarks/`
2. Create schema JSON file in `data/benchmarks/`
3. Add entry to `data/benchmarks.json` (for full benchmarks) or `data/test-benchmarks.json` (for test/development benchmarks)
4. Update documentation

**Note:** Test benchmarks should be smaller subsets (3-50 questions) suitable for quick validation and CI/CD pipelines. Place test benchmark data files in `data/benchmarks/test_benchmarks/`.

### Adding a New Evaluation Metric

1. Add metric function to `src/text2sql_eval_toolkit/evaluation/evaluation_tools.py`
2. Add tests in `tests/test_evaluation.py`
3. Update documentation
4. Add example usage in notebooks

### Adding Database Support

1. Add connection logic to `src/text2sql_eval_toolkit/execution/execution_tools.py`
2. Add optional dependencies to `pyproject.toml`
3. Update `env.example` with required environment variables
4. Add tests for the new database type
5. Update README with setup instructions

## Testing Guidelines

- Write unit tests for new functions
- Write integration tests for end-to-end workflows
- Use fixtures for common test data
- Mock external services (LLM APIs, databases) when appropriate
- Aim for >80% code coverage

## Commit Message Guidelines

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(evaluation): add F1 score metric for SQL comparison

fix(execution): handle NULL values in MySQL queries

docs(readme): update installation instructions for Python 3.12
```

## Release Process

Releases are managed by project maintainers. Everything after the tag is
automated -- there is no step to remember:

1. Update the version in `pyproject.toml`.
2. Add the section to `CHANGELOG.md` under a `## [x.y.z] - YYYY-MM-DD` heading.
   This is **required**: the release workflow extracts the page's notes from it
   and fails the release if the version has no section.
3. `uv lock` (the lockfile records the project's own version).
4. Tag: `git tag -a v1.5.0 -m "Release v1.5.0"`
5. Push: `git push origin v1.5.0`

`release.yml` then builds, checks the tag against the packaged version, checks
the wheel carries its UI and judge configs, publishes to PyPI through Trusted
Publishing (gated on the `pypi` environment, so it waits for a reviewer), and
creates the GitHub Release with the changelog notes and the built artifacts
attached.

To preview the notes a tag would produce:

```bash
python scripts/ci/extract_changelog.py v1.5.0
```

## Getting Help

- **Documentation**: Check the [README](README.md) and inline documentation
- **Issues**: Search existing issues or create a new one
- **Discussions**: Use GitHub Discussions for questions and ideas

## Recognition

Contributors will be recognized in:
- The project README
- Release notes
- Git commit history

Thank you for contributing to the Text2SQL Evaluation Toolkit!