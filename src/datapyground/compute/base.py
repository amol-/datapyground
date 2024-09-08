"""Base classes and interfaces for Compute Engine

This module defines the base components that are
necessary to represent a query plan and execute it.
"""

import abc
from typing import Iterator

import pyarrow as pa


class QueryPlanNode(abc.ABC):
    """A node of a query execution plan.

    The Query plan is represented as a tree
    of nodes. Each node is a step in the execution
    and all previous steps are children of the
    last one.

    For example a simple plan might involve
    loading data and filtering it::

        LoadDataNode -> FilterDataNode(filter)

    That would be a plan where the last step
    is filtering, and the LoadDataNode is a child
    of the filter node.

    The number of children can be variable, some
    nodes like for example Joins, will accept one or
    more child nodes that have to be joined together.

    Each Node accepts :class:`pyarrow.RecordBatch`
    data as its input and emits a new
    :class:`pyarrow.RecordBatch` as its output.

    The base `QueryPlanNode` class does nothing
    and purely acts as the interface that all nodes
    must implement. Actual work will be done
    in the subclasses.

    For example a simple node that takes data
    and just forwards it as is after printing
    its content can be implemented as::

        class DebugDataNode(QueryPlanNode):
            def __init__(self, child):
                self.child = child

            def batches(self):
                for b in self.child.batches():
                    print(b)
                    yield b

            def __str__(self):
                return f"DebugDataNode()"
    """

    @abc.abstractmethod
    def batches(self) -> Iterator[pa.RecordBatch]:
        """Emits the batches for the next node.

        Each QueryPlan node is expected to be able to
        generate data that has to be provided to the next
        node in the plan.

        Usually this happens by consuming data from its
        child nodes, transforming it somehow, and yielding
        it back to the next consumer.
        """
        ...

    @abc.abstractmethod
    def __str__(self) -> str:
        """Human readable representation of the node."""
        ...


class Expression(abc.ABC):
    """Expression to apply to a RecordBatch.

    Expressions are some form of operation that
    has to be applied to the data of a :class:`pyarrow.RecordBatch`
    to create new data.

    Typical example of expressions are: A + B
    which is expected to sum column A of the RecordBatch
    to column B of the RecordBatch and return the result.

    As our engine is Column Major, applying an expression
    always results in a new column, thus in a
    :class:`pyarrow.Array` that contains the data
    for that column.
    """

    @abc.abstractmethod
    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        """Apply the expression to a RecordBatch.

        Expression classes must implement this method
        to dictate what will happen when an expression
        is applied.

        Suppose want to implement a ``SumExpression`` class
        that might look like::

            class SumExpression(Expression):
                def __init__(self, leftcol, rightcol):
                    self.lcol = lcol  # left column name
                    self.rcol = rcol  # right column name

                def apply(self, batch):
                    return pyarrow.compute.sum(
                        batch[self.lcol],
                        batch[self.rcol]
                    )
        """
        ...

    @abc.abstractmethod
    def __str__(self) -> str:
        """Human readable representation of the expression."""
        ...


class ColumnRef(Expression):
    """References a column in a record batch.

    When another expression or the engine need
    to operate on a specific column, we will
    need a way to reference that column and its data.

    This expression is aware of the column and when
    applied to a record batch returns the data for
    that column.
    """

    def __init__(self, name: str) -> None:
        """
        :param name: The name of the column being referenced.
        """
        self.name = name

    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        """Get the data for the column."""
        return batch.column(self.name)

    def __str__(self) -> str:
        return f"ColumnRef({self.name})"


col = ColumnRef
