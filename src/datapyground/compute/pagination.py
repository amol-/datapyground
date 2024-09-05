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
        consumed_rows = 0  # keep track of how many rows we have already seen

        for batch in self.child.batches():
            batch_size = batch.num_rows

            # Keep discarding batches until we get to the batch that
            # has the rows _after_ offset.
            if consumed_rows + batch_size <= self.offset:
                consumed_rows += batch_size
                continue

            # As the rows we care about might be further on
            # inside the batch, check if we have to start
            # picking rows at the beginning or if we have to discard
            # some rows of the batch.
            start_in_batch = max(0, self.offset - consumed_rows)

            # Now that we know where to start in the batch, 
            # we need to compute where to end.
            # The batch might actually contain fewer rows than
            # length so we might have to keep picking rows
            # from subsequent batches. 
            remaining_rows = self.offset + self.length - consumed_rows
            rows_in_this_batch = min(batch_size - start_in_batch, remaining_rows)
            if rows_in_this_batch > 0:
                yield batch.slice(start_in_batch, rows_in_this_batch)
            consumed_rows += batch_size
            if consumed_rows >= self.offset + self.length:
                break