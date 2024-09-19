"""The DataPyground Compute Engine

The compute engine defines the in-memory
format for query plans and the plan nodes
supported.

The compute engine is tightly bound to Apache Arrow,
thus the engine will expect to always deal with
:class:`pyarrow.RecordBatch` and emit a new RecordBatch
as the result of the node execution.

This allows to easily build compute pipelines like::

    (RecordBatch)-->Node1--(RecordBatch)-->Node2--(RecordBatch)-->...

The query plan nodes themselves are in charge of their execution,
this keeps the behavior near to the node and thus makes easy to
know how a Node is actually executed without having to look around too much.

Building a query plan requires to combine the nodes that we want
to be executed starting with one or ore ``DataSource`` nodes as the
leafs node of a query:

>>> import pyarrow as pa
>>> data = pa.table({
...    "animals": pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"]),
...    "n_legs": pa.array([2, 4, 5, 100])
... })
>>>
>>> import pyarrow.compute as pc
>>> from datapyground.compute import col, PyArrowTableDataSource
>>> from datapyground.compute import FilterNode, FunctionCallExpression
>>> # SELECT * FROM data WHERE n_legs >= 5
>>> query = FilterNode(
...     FunctionCallExpression(pc.greater_equal, col("n_legs"), 5),
...     child=PyArrowTableDataSource(
...         data
...     )
... )
>>> for data in query.batches():
...     print(data)
pyarrow.RecordBatch
animals: string
n_legs: int64
----
animals: ["Brittle stars","Centipede"]
n_legs: [5,100]
"""

from .aggregate import (
    AggregateNode,
    CountAggregation,
    MaxAggregation,
    MeanAggregation,
    MinAggregation,
    SumAggregation,
)
from .base import ColumnRef, col, lit
from .datasources import CSVDataSource, PyArrowTableDataSource
from .expressions import FunctionCallExpression
from .filtering import FilterNode
from .pagination import PaginateNode
from .selection import ProjectNode
from .sorting import ExternalSortNode, SortNode

__all__ = (
    "CSVDataSource",
    "PyArrowTableDataSource",
    "FilterNode",
    "FunctionCallExpression",
    "col",
    "lit",
    "ColumnRef",
    "PaginateNode",
    "SortNode",
    "ExternalSortNode",
    "ProjectNode",
    "AggregateNode",
    "CountAggregation",
    "MaxAggregation",
    "MeanAggregation",
    "MinAggregation",
    "SumAggregation",
)
