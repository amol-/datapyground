<img src="docs/logo.png" alt="DataPyground" width="180"/>

# DataPyground

[![Tests](https://img.shields.io/github/actions/workflow/status/amol-/datapyground/pytest.yml?branch=main&label=tests)](https://github.com/amol-/datapyground/actions)
[![Coverage](https://img.shields.io/coveralls/github/amol-/datapyground)](https://coveralls.io/github/amol-/datapyground)

Data Analysis framework and Compute Engine for fun,
it was started as a foundation for the [**How Data Platforms Work**](https://github.com/amol-/datapyground/tree/main/book)
book associated to the [**Monthly Python Data Engineering Newsletter**](https://alessandromolina.substack.com/) 
while writing the book to showcase the concepts explained in the its chapters.

The main priority of the codebase is to be as feature complete
as possible while making it easy to understand and contribute to 
for people that have no prior knowledge of compute
engines or data processing frameworks in general.

The codebase is heavily documented and commented to make it easy to understand
and modify, and contributions are welcomed and encouraged, it is meant
to be a safe playground for learning and experimentation.

## Documentation

Each component of the data platform is self documented in a way inspired
by the literate programming concept. The complete documentation
is available at [Documentation](http://alessandro.molina.fyi/datapyground/)

For further understanding of the codebase and the concepts
reading the [**How Data Platforms Work**](https://github.com/amol-/datapyground/tree/main/book) 
book is recommended.

## Getting Started

Install datapyground package from pip:

```bash
pip install datapyground
```

Once installed refer to the [Documentation](http://alessandro.molina.fyi/datapyground/) 
of each component to learn how to use it.

### Commands

`DataPyground` exposes some commands to play around with its features,
currently the following commands are provided:

- `pyground-fquery` which allows to run SQL queries on CSV and Parquet files.

## Contributing

Contributions are welcomed and encouraged, it is meant
to be a safe playground for learning and experimentation.

The only requirement is that the contributions maintain
or increase the level of quality of the documentation and codebase,
contributions that are not properly documented won't be merged,
consider quality of docmentation more important that elegance or performance
of the codebase for this project.

The contributions are currently meant to be in **pure python**,
this does not prevent the use of c extensions and cython for performance
in the future, but that will have to happen when the benefit they provide
outweights the added complexity they introduce in the context of a learning
project.

### Setup development environment

Install `uv` python package:

```bash
pip install uv
```

Then install the dependencies and the project in editable mode:

```bash
uv sync --dev
```

### Running tests

```bash
uv run pytest -v
```

### Building Docs

```bash
cd docs
uv run make html
```

The documentation is readable at ``docs/build/html``
after being built.
