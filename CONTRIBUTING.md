# Contributing to Koality

Thank you for your interest in contributing to Koality! We welcome contributions from the community.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to pit@ottogroup.com.

## How to Contribute

### Reporting Bugs

If you find a bug, please [open an issue](https://github.com/ottogroup/koality/issues) with:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs. actual behavior
- Your environment (Python version, OS, koality version)

### Suggesting Features

Feature suggestions are welcome! Please [open an issue](https://github.com/ottogroup/koality/issues) describing:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

### Submitting Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Set up your development environment** (see below)
3. **Make your changes** and ensure tests pass
4. **Submit a pull request** with a clear description of your changes

## Development Setup

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Installation

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/koality.git
cd koality

# Create a virtual environment and install dependencies
uv sync --group dev --group docs

# Or with pip
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev,docs]"
```

### Running Tests

```bash
# Run all tests
poe test

# Run unit tests only
poe test_unit

# Run integration tests only
poe test_integration
```

### Code Quality

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Format code
poe format

# Check linting
poe lint
```

### Security Checks

```bash
# Check for vulnerable dependencies
poe check_vulnerable_dependencies

# Check for unused dependencies
poe check_unused_dependencies

# Check GitHub Actions security
poe check_githubactions
```

### Documentation

```bash
# Serve docs locally
poe docs_serve
```

## Pull Request Guidelines

- Follow the existing code style
- Write clear commit messages
- Add tests for new functionality
- Update documentation as needed
- Keep pull requests focused on a single change
- Ensure all CI checks pass

## Project Structure

```
koality/
├── src/koality/     # Main source code
├── tests/           # Test files
├── docs/            # Documentation
└── pyproject.toml   # Project configuration
```

## Questions?

Feel free to [open an issue](https://github.com/ottogroup/koality/issues) or reach out to the maintainers.

Thank you for contributing!