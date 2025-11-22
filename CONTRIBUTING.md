# Contributing to Financial Analysis Toolkit

Thank you for your interest in contributing to the Financial Analysis Toolkit! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork the repository** and clone your fork locally
2. **Create a virtual environment** and install development dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```
3. **Install pre-commit hooks**:
   ```bash
   pre-commit install
   ```

## Development Workflow

1. **Create a branch** for your feature or bug fix:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make your changes** following our coding standards

3. **Run tests** to ensure everything works:
   ```bash
   make test
   # or
   pytest
   ```

4. **Run linting and formatting**:
   ```bash
   make lint
   make format
   ```

5. **Commit your changes** with clear commit messages:
   ```bash
   git commit -m "Add: description of your feature"
   ```

6. **Push to your fork** and create a Pull Request

## Coding Standards

### Python Style Guide

- Follow PEP 8 style guidelines
- Use Black for code formatting (line length: 100)
- Use isort for import sorting
- Maximum line length: 100 characters

### Code Formatting

Run the formatters before committing:
```bash
make format
```

Or manually:
```bash
black .
isort .
```

### Type Hints

- Use type hints for function parameters and return values when possible
- Use `typing` module for complex types

### Docstrings

- Use Google-style docstrings
- Document all public functions, classes, and modules
- Include parameter descriptions and return value descriptions

Example:
```python
def calculate_returns(prices: pd.Series) -> pd.Series:
    """Calculate returns from price series.

    Args:
        prices: Series of asset prices

    Returns:
        Series of calculated returns
    """
    return prices.pct_change()
```

### Testing

- Write tests for all new features
- Aim for high test coverage
- Place tests in the `tests/` directory
- Use descriptive test names: `test_<function_name>_<scenario>`

Example:
```python
def test_calculate_returns_with_valid_input():
    """Test calculate_returns with valid price series."""
    prices = pd.Series([100, 110, 105, 120])
    returns = calculate_returns(prices)
    assert len(returns) == len(prices)
    assert pd.isna(returns.iloc[0])
```

### Logging

- Use the logging module instead of print statements
- Use appropriate log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Use structured logging through the utils.logging module

Example:
```python
from utils.logging import get_logger

logger = get_logger(__name__)

def my_function():
    logger.info("Starting operation")
    try:
        # operation code
        logger.debug("Operation details")
    except Exception as e:
        logger.error(f"Operation failed: {e}", exc_info=True)
```

## Commit Messages

Use clear, descriptive commit messages following this format:

```
<type>: <subject>

<body>

<footer>
```

Types:
- `Add:` - New feature
- `Fix:` - Bug fix
- `Update:` - Updates to existing features
- `Refactor:` - Code refactoring
- `Docs:` - Documentation changes
- `Test:` - Test additions or changes
- `Style:` - Code style changes (formatting, etc.)

Examples:
```
Add: Implement news scraping functionality

- Added NewsScraper class
- Integrated with PostgreSQL storage
- Added error handling and retry logic

Fix: Resolve database connection timeout issue

Update: Improve error messages for better debugging
```

## Pull Request Process

1. **Update documentation** if you've changed functionality
2. **Add tests** for new features
3. **Ensure all tests pass** locally
4. **Update CHANGELOG.md** with your changes (if applicable)
5. **Create a Pull Request** with:
   - Clear title and description
   - Reference to related issues
   - Description of changes and testing performed

## Review Process

- All PRs require at least one approval before merging
- Address review comments promptly
- Keep PRs focused and reasonably sized
- Respond to CI/CD failures

## Questions?

If you have questions or need help, please:
- Open an issue for discussion
- Check existing documentation
- Ask in the project discussions

Thank you for contributing!

