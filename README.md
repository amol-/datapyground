<img src="docs/logo.png" alt="DataPyground" width="180"/>

# DataPyground

[![Tests](https://img.shields.io/github/actions/workflow/status/amol-/datapyground/pytest.yml?branch=main&label=tests)](https://github.com/amol-/datapyground/actions)
[![Coverage](https://img.shields.io/coveralls/github/amol-/datapyground)](https://coveralls.io/github/amol-/datapyground)

Data Analysis framework and Compute Engine for fun,
it was started as a foundation for the **How Data Platforms Work**
book while writing it to showcase the concepts explained in the book.

The main priority of the codebase is to be as feature complete
as possible while making it easy to understand and contribute to 
for people that have no prior knowledge of compute
engines or data processing frameworks in general.

The codebase is heavily documented and commented to make it easy to understand
and modify, and contributions are welcomed and encouraged, it is meant
to be a safe playground for learning and experimentation.

## Getting Started

Install datapyground package from pip:

```bash
pip install datapyground
```

Then you can run it via the Query Plan interface:

```python
import pyarrow.compute as pc

from datapyground.compute import FilterNode, CSVDataSource, col, FunctionCallExpression

query = FilterNode(
    FunctionCallExpression(pc.equal, col("City"), "Rome"),
    CSVDataSource("data/shops.csv")
)
for batch in query.batches():
    print(batch)
```

Or via the Dataframe API:

```python
import pyarrow.compute as pc

from datapyground.compute import col, FunctionCallExpression
from datapyground.dataframe import Dataframe

Dataframe.open("shops.csv") \
  .filter(FunctionCallExpression(pc.equal, col("City"), "Rome")) \
  .collect()
```

## Documentation

The documentation is under progress.

For further understanding of the codebase and the concepts
reading the **How Data Platforms Work** book is recommended.

**NOTE:** Book is under progress.

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
