# Project Overview

This is a Python toolbox containing various modules and utilities. The project is managed with Poetry and includes modules for interacting with Google Cloud services like BigQuery and GDrive, as well as helper functions for tasks like caching, plotting, and string conversion.

# Building and Running

## Dependencies

The project uses Poetry for dependency management. To install the required dependencies, run:

```bash
poetry install
```

## Testing

The project uses pytest for testing. To run the tests, you can use the following command:

```bash
poetry run pytest
```

You can also run specific test files:

```bash
poetry run pytest tests/test___caching.py
```

## Building

The project can be built using the `scripts-build.py` script.

# Development Conventions

*   The project uses `black` for code formatting.
*   The `scripts-devtools.py` script contains utility functions for development, such as a function to check BigQuery datasets.
