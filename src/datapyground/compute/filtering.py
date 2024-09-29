"""Query plan nodes that implement filtering of rows.

A common request in queries is to filter the data to
pick only the rows that respect a specific filter.
An example is the ``WHERE`` condition in SQL queries.

This module implements the basic filtering capabilities.
"""

from .base import QueryPlanNode
from .expressions import Expression


class FilterNode(QueryPlanNode):
    """Filter data based on a predicate expression.

    The filter expects an expression that when applied
    to the batch of data being filtered returns ``true``
    or ``false`` for each row in the data to mark which
    rows have to be preserved and which rows have to be discarded.

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> from datapyground.compute import col, lit, FunctionCallExpression, PyArrowTableDataSource
    >>> data = pa.record_batch({"values": [1, 2, 3, 4, 5]})
    >>> predicate = FunctionCallExpression(pc.greater, col("values"), lit(3))
    >>> # predicate is a function that returns true for values greater than 3
    >>> predicate.apply(data)
    <pyarrow.lib.BooleanArray object at ...>
    [
      false,
      false,
      false,
      true,
      true
    ]
    >>> next(FilterNode(predicate, PyArrowTableDataSource(data)).batches())
    pyarrow.RecordBatch
    values: int64
    ----
    values: [4,5]
    """

    def __init__(self, expression: Expression, child: QueryPlanNode) -> None:
        """
        :param expression: The predicate expression to filter with.
        :param child: The node emitting the data to be filtered.
        """
        self.expression = expression
        self.child = child

    def __str__(self) -> str:
        return f"FilterNode(filter={self.expression}, child={self.child})"

    def batches(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Apply the filtering to the child node.

        For each recordbatch yielded by the child node,
        apply the expression and get back a mask
        (an array of only true/false values).

        Based on the mask filter the rows of the batch
        and return only those matching the filter.
        """
        for batch in self.child.batches():
            mask = self.expression.apply(batch)
            yield batch.filter(mask)
