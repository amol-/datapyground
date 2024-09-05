from typing import Iterator

import pyarrow as pa

from .base import QueryPlanNode


class PaginateNode(QueryPlanNode):
    def __init__(self, offset: int, length: int, child: QueryPlanNode) -> None:
        self.offset = offset
        self.length = length
        self.end = offset + length
        self.child = child

    def __str__(self) -> str:
        return f"PaginateNode({self.start}:{self.end}, {self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        current = 0
        for batch in self.child.batches():
            while current < self.offset:
                current += batch.num_rows
                batch = batch.slice(1)
            if current > self.end:
                break
            yield batch
            current += batch.num_rows