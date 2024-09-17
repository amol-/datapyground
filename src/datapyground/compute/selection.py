"""Query plan nodes that implement projection of columns.

A common request in queries is to select specific columns
and project new columns based on expressions.
An example is the ``SELECT`` clause in SQL queries.

This module implements the basic projection capabilities.
"""

from typing import Iterator

import pyarrow as pa

from .base import QueryPlanNode
from .expressions import Expression


class ProjectNode(QueryPlanNode):
    """Project data by selecting specific columns and computing expressions.

    The projection expects a list of column names to select and a dictionary
    of column names and expressions to project new columns.

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> from datapyground.compute import col, lit, FunctionCallExpression, PyArrowTableDataSource
    >>> data = pa.record_batch({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> next(ProjectNode(["a"], {"ab_sum": FunctionCallExpression(pc.add, col("a"), col("b"))},
    ...                  PyArrowTableDataSource(data)).batches())
    pyarrow.RecordBatch
    a: int64
    ab_sum: int64
    ----
    a: [1,2,3]
    ab_sum: [5,7,9]
    """

    def __init__(
        self,
        select: list[str] | None,
        project: dict[str, Expression] | None,
        child: QueryPlanNode,
    ) -> None:
        """
        :param columns: The list of column names to select.
                        ``None`` means select all columns.
                        ``[]`` means select only the projected columns.
        :param expressions: The dict {name: Expression} to project new columns.
        :param child: The node emitting the data to be projected.
        """
        self.select = select
        self.project = project or {}
        self.child = child

        if self.select is None:
            # No selection was provided, we will select all columns
            self.restrict_columns = None
        else:
            # This is the list of columns we want to keep,
            # in case select=[] it will only provide the project columns.
            self.restrict_columns = self.select + list(self.project.keys())

    def __str__(self) -> str:
        return f"ProjectNode(select={self.select}, project={self.project}, child={self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        """Apply the projection to the child node.

        For each recordbatch yielded by the child node,
        sequentially apply the expressions to project new columns
        and then select the requested columns.

        We need to do this
        """
        for batch in self.child.batches():
            for name, expr in self.project.items():
                batch = batch.append_column(name, expr.apply(batch))

            if self.restrict_columns is not None:
                batch = batch.select(self.restrict_columns)

            yield batch
