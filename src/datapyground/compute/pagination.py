"""Support limiting or skipping data in a query plan.

Implements nodes whose purpose is to slice the data
emitted by a query plan. Discarding the rows that
are not part of the selected slice of data.
"""

from typing import Iterator

import pyarrow as pa

from .base import QueryPlanNode


class PaginateNode(QueryPlanNode):
    """Emit only one page of the received data.

    Given a starting index and a length, only emit
    length rows after the starting index is reached.

    For example if ``offset=1`` and ``length=1``
    onlt the second row will be emitted::

        0: skip because < offset
        1: emit
        2: skip because > length=1 and one row was already emitted.
    """

    def __init__(self, offset: int, length: int, child: QueryPlanNode) -> None:
        """
        :param offset: From which row to take data, first row is 0.
        :param length: How many rows to take after offset was reached.
        :param child: the node from which to consume the rows.
        """
        self.offset = offset
        self.length = length
        self.end = offset + length
        self.child = child

    def __str__(self) -> str:
        return f"PaginateNode({self.offset}:{self.end}, {self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        """Apply the pagination to the child node and emit the rows.

        Consume rows from the child node skipping those until we
        reach offset. Once offset is reached start yielding rows
        until length is reached.

        Subsequent rows are never consumed, so the child might
        not get exhausted. This requires special attention in
        resources management, because any resource open by the
        child might remain unclosed if the child waits for all
        the data to be consumed before closing it.
        """
        consumed_rows = 0  # keep track of how many rows we have already seen

        batches_generator = self.child.batches()
        for batch in batches_generator:
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
                batches_generator.close()
                break
