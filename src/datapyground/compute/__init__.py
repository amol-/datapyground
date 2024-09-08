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
"""
from .base import ColumnRef, col
from .datasources import CSVDataSource, PyArrowTableDataSource
from .expressions import FunctionCallExpression
from .filtering import FilterNode
from .pagination import PaginateNode

__all__ = (
    "CSVDataSource",
    "PyArrowTableDataSource",
    "FilterNode",
    "FunctionCallExpression",
    "col",
    "ColumnRef",
    "PaginateNode",
)