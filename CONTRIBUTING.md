# Contributing to Sentri

Thank you for your interest in contributing to Sentri! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Python 3.8 or higher
- Git

### Setting Up Development Environment

1. **Clone the repository**
   ```bash
   git clone https://github.com/milind-kulshrestha/sentri.git
   cd sentri
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

## Development Workflow

### Code Quality Standards

We maintain high code quality standards using automated tools:

- **Formatting**: Black (line length: 88)
- **Import sorting**: isort
- **Linting**: flake8
- **Type checking**: mypy
- **Security**: bandit
- **Testing**: pytest with coverage

### Running Quality Checks

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type check
mypy src/

# Security scan
bandit -r src/

# Run tests with coverage
pytest tests/ --cov=src/data_quality --cov-report=term-missing
```

### Pre-commit Workflow

Before submitting a pull request:

1. **Run all quality checks**
   ```bash
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   mypy src/
   bandit -r src/
   ```

2. **Run full test suite**
   ```bash
   pytest tests/ --cov=src/data_quality --cov-report=term-missing
   ```

3. **Ensure coverage is maintained**
   - Aim for >90% test coverage
   - Add tests for new functionality
   - Update tests for modified functionality

## Contributing Guidelines

### Types of Contributions

- **Bug fixes**: Fix existing functionality
- **New features**: Add new data quality checks or connectors
- **Documentation**: Improve or add documentation
- **Performance**: Optimize existing code
- **Tests**: Add or improve test coverage

### Pull Request Process

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow existing code patterns
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**
   ```bash
   pytest tests/ -v
   ```

4. **Commit with clear messages**
   ```bash
   git commit -m "feat: add new completeness check feature"
   ```

5. **Push and create pull request**
   ```bash
   git push origin feature/your-feature-name
   ```

### Commit Message Format

Use conventional commit format:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Adding or updating tests
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `chore:` Maintenance tasks

### Code Style Guidelines

1. **Python Style**
   - Follow PEP 8
   - Use Black formatter (88 character line length)
   - Use type hints for all functions
   - Add docstrings for all public methods

2. **Testing**
   - Write unit tests for all new functionality
   - Use pytest fixtures for common test data
   - Mock external dependencies
   - Aim for edge case coverage

3. **Documentation**
   - Update README.md for user-facing changes
   - Add docstrings with examples
   - Update configuration documentation

## Adding New Data Quality Checks

### Check Implementation

1. **Create check class**
   ```python
   from data_quality.checks.base import BaseCheck
   
   class MyNewCheck(BaseCheck):
       def run(self) -> pd.DataFrame:
           # Implementation
           pass
   ```

2. **Add to check registry**
   ```python
   # In src/data_quality/checks/__init__.py
   from .my_new_check import MyNewCheck
   ```

3. **Write comprehensive tests**
   ```python
   # In tests/unit/test_my_new_check.py
   class TestMyNewCheck:
       def test_basic_functionality(self):
           # Test implementation
           pass
   ```

4. **Update documentation**
   - Add to README.md check types table
   - Add configuration examples
   - Update requirements document

### Adding New Connectors

1. **Inherit from base connector**
   ```python
   from data_quality.connectors.base import DataConnector
   
   class MyNewConnector(DataConnector):
       def validate_connection(self) -> bool:
           # Implementation
           pass
       
       def get_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
           # Implementation
           pass
   ```

2. **Register connector**
   ```python
   from data_quality.connectors.registry import register_connector
   
   @register_connector('my_new_source')
   class MyNewConnector(DataConnector):
       # Implementation
   ```

3. **Add tests and documentation**

## Release Process

### Version Bumping

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release PR
4. Tag release after merge

### Publishing

Releases are automatically published to PyPI via GitHub Actions when a release is created.

## Getting Help

- **Issues**: Create GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Email**: Contact maintainer at milind.kulshrestha@gmail.com

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow the Golden Rule

Thank you for contributing to Sentri! 🚀
