from typing import Iterator

import pyarrow as pa

from .base import QueryPlanNode
from .expressions import Expression


class FilterNode(QueryPlanNode):
    def __init__(self, expression: Expression, child: QueryPlanNode) -> None:
        self.expression = expression
        self.child = child

    def __str__(self) -> str:
        return f"FilterNode({self.expression}, {self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self.child.batches():
            mask = self.expression.apply(batch)
            yield batch.filter(mask)