"""Query plan nodes that perform sorting of data.

When computing ranks or looking for most significant
values, it's often necessary to sort the data based
on one or more columns.

Sorted data can also benefit aggregations which
will be able to run faster as all the values that
constitute a group are in succession.

This module implements the sorting capabilities.
"""

import heapq
import os
from tempfile import NamedTemporaryFile
from typing import Iterator, Self

import pyarrow as pa
import pyarrow.compute as pc

from .base import QueryPlanNode


class SortNode(QueryPlanNode):
    """Sort data in-memory based on one or more columns.

    The node expects a list of columns and a list of
    sort directions. The data will be sorted based on
    the columns in the order they are provided.

    The sort directions are used to specify if the
    sorting should be ascending or descending.

    >>> import pyarrow as pa
    >>> from datapyground.compute import col, lit, FunctionCallExpression, PyArrowTableDataSource
    >>> data = pa.record_batch({"values": [1, 2, 3, 4, 5]})
    >>> # Sort the data in descending order
    >>> sort = SortNode(["values"], [True], PyArrowTableDataSource(data))
    >>> next(sort.batches())
    pyarrow.RecordBatch
    values: int64
    ----
    values: [5,4,3,2,1]
    """

    def __init__(
        self, keys: list[str], descending: list[bool], child: QueryPlanNode
    ) -> None:
        """
        :param keys: The columns to sort by in the order they should be sorted.
        :param descending: If each columns should be sorted in a descending order.
        :param child: The node emitting the data to be filtered.
        """
        if len(keys) != len(descending):
            raise ValueError("Keys and descending must have the same length")

        self.sorting = list(
            zip(keys, ("descending" if desc else "ascending" for desc in descending))
        )
        self.child = child

    def __str__(self) -> str:
        return f"SortNode(sorting={self.sorting}, {self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        """The sorting to the child node.

        Batches provided by child node are accumulated
        until they are all loaded in memory, than they
        are merged and sorted as an unique table.

        This is usually faster but requires more memory
        and might oom for large datasets.
        """
        batches = list(self.child.batches())
        if len(batches) == 1:
            yield batches[0].sort_by(self.sorting)

        # The process converts the batches to tables
        # as converting to and from tables is a zero-copy
        # operation and tables can be concatenated at no cost
        # when promote_options is set to none as they are based on ChunkedArrays.
        table = pa.concat_tables(
            [pa.table(batch) for batch in batches], promote_options="none"
        )
        table = table.sort_by(self.sorting)
        # to_batches is a zero-copy operation when maximum chunk size is None
        for batch in table.to_batches():
            try:
                yield batch
            except GeneratorExit:
                break


class ExternalSortNode(QueryPlanNode):
    """Sort data based on one or more columns offloading to disk.

    This node behaves similarly to :class:`SortNode` but
    performs the sorting operation offloading the data
    to disk. It expects each chunk to fit into memory
    individually, which is a reasonable expectation given
    that the child node had generated them without running
    out of memory, but it can work with datasets that are
    larger than the available memory.

    >>> import pyarrow as pa
    >>> from datapyground.compute import col, lit, FunctionCallExpression, PyArrowTableDataSource
    >>> data = pa.record_batch({"values": [1, 2, 3, 4, 5]})
    >>> # Sort the data in descending order
    >>> sort = SortNode(["values"], [True], PyArrowTableDataSource(data))
    >>> next(sort.batches())
    pyarrow.RecordBatch
    values: int64
    ----
    values: [5,4,3,2,1]
    """

    _TEMPORARY_FILE_PREFIX = "datapyground_"

    def __init__(
        self,
        keys: list[str],
        descending: list[bool],
        child: QueryPlanNode,
        batch_size: int = 1024,
    ) -> None:
        """
        :param keys: The columns to sort by in the order they should be sorted.
        :param descending: If each columns should be sorted in a descending order.
        :param child: The node emitting the data to be filtered.
        """
        if len(keys) != len(descending):
            raise ValueError("Keys and descending must have the same length")

        self.batch_size = batch_size
        self.sorting_keys = keys
        self.descending_orders = descending
        self.sorting = list(
            zip(keys, ("descending" if desc else "ascending" for desc in descending))
        )
        self.child = child

    def __str__(self) -> str:
        return f"SortNode(sorting={self.sorting}, {self.child})"

    def batches(self) -> Iterator[pa.RecordBatch]:
        """The sorting to the child node.

        Each batch yielded by the child node will be
        sorted individually based on the provided keys
        then the result will be merged.

        This requires loading in memory all the data.
        Currently pyarrow doesn't seem to offer any way
        to implement an external sorting algorithm.

        The memory usage is kept at the minimum by using
        temporary files to store the batches and memory
        mapping them to avoid loading all the data at once,
        but the data will be fully loaded in when the batches
        are merged.

        Also this node can leak temporary files
        if the generator is not closed.
        """
        temporay_files = []
        mmaped_batches = []
        try:
            for batch in self.child.batches():
                # Pre-sort the batch, that should make the subsequent
                # sort_by faster. Ideally we would need a merge operation
                # in PyArrow that allows to merge presorted batches,
                # but that is curently not available.
                sorted_batch = batch.sort_by(self.sorting)
                # Memory map the batches so that we don't need to manage
                # memory ourselves.
                batch_file_name, mmaped_batch = self._memory_map_batch(sorted_batch)
                temporay_files.append(batch_file_name)
                mmaped_batches.append(mmaped_batch)

            # For sorting without having to load all data in memory
            # we will use an heap, it will take care of the order of
            # the rows of each batch for us.
            heap = []

            # Fill the heap with the first row of each batch.
            for idx, mmaped_batch in enumerate(mmaped_batches):
                first_row = mmaped_batch.slice(0, 1)
                key_values = ExternalSortKey.get_sort_key_values(
                    first_row, self.sorting_keys
                )
                sort_key = ExternalSortKey(key_values, self.descending_orders)
                heapq.heappush(heap, (sort_key, idx, 0, first_row))

            # Now that we have an heap ready, we pull one row at a time
            # out of the heap. The heap will always return the first
            # sorted according to the ExternalSortKey.
            # After we have consumed one row from the heap, we push
            # back a new row from the same batch, if there are any.
            # This way the heap will always have one row from each batch.
            batch_schema = mmaped_batches[0].schema
            current_batch = [[] for _ in range(len(batch_schema))]
            while heap:
                _, batch_idx, row_idx, row = heapq.heappop(heap)
                for idx, column in enumerate(row.columns):
                    current_batch[idx].append(column[0])

                # If we accumulated enough rows that it makes
                # sense to yield a batch, we do so.
                if len(current_batch[0]) >= self.batch_size:
                    chunk = pa.record_batch(current_batch, schema=batch_schema)
                    try:
                        yield chunk
                    except GeneratorExit:
                        return
                    for column in current_batch:
                        column.clear()

                mmaped_batch = mmaped_batches[batch_idx]
                next_row_id = row_idx + 1
                if next_row_id >= len(mmaped_batch):
                    continue
                next_row = mmaped_batch.slice(next_row_id, 1)
                key_values = ExternalSortKey.get_sort_key_values(
                    next_row, self.sorting_keys
                )
                sort_key = ExternalSortKey(key_values, self.descending_orders)
                heapq.heappush(heap, (sort_key, batch_idx, next_row_id, next_row))

            # Yield one last batch with the remaining rows.
            if len(current_batch[0]):
                chunk = pa.record_batch(current_batch, schema=batch_schema)
                try:
                    yield chunk
                except GeneratorExit:
                    return
        finally:
            # Clean up the temporary files and memory maps.
            for temp_file in temporay_files:
                os.unlink(temp_file)

    def _memory_map_batch(
        self, record_batch: pa.RecordBatch
    ) -> tuple[str, pa.RecordBatch]:
        with NamedTemporaryFile(
            prefix=self._TEMPORARY_FILE_PREFIX, delete=False
        ) as batch_file:
            batch_file_name = batch_file.name
            with pa.ipc.RecordBatchFileWriter(
                batch_file, record_batch.schema
            ) as writer:
                writer.write_batch(record_batch)

        # We can immediately close the file as according to POSIX:
        # The mmap() function shall add an extra reference to the file associated
        # with the file descriptor which is not removed by a subsequent close()
        # on that file descriptor.
        # This reference shall be removed when there are no more mappings to the file.
        with pa.memory_map(batch_file_name, "r") as mmapped_file:
            with pa.ipc.open_file(mmapped_file) as reader:
                assert reader.num_record_batches == 1
                data = reader.get_batch(0)
        return batch_file_name, data


class ExternalSortKey:
    """Makes pyarrow data sortable by Python functions.

    This implements the rich comparison methods to allow
    sorting of pyarrow data based on the values of the
    columns in the order they are provided.
    """

    SCALAR_TRUE = pa.scalar(True)

    def __init__(self, values: list[pa.Scalar], descending_orders: list[bool]) -> None:
        """
        :param values: The values to compare.
        :param descending_orders: Which of the values are compared for descending order
        """
        self.values = values
        self.descending_orders = descending_orders

    @classmethod
    def get_sort_key_values(
        cls, record_batch: pa.RecordBatch, sorting_keys: list[str]
    ) -> list[pa.Scalar]:
        """Extract sort key values from a RecordBatch containing a single row."""
        values = []
        for key in sorting_keys:
            index = record_batch.schema.get_field_index(key)
            column = record_batch.column(index)
            value = column[0]
            values.append(value)
        return values

    def __lt__(self, other: Self) -> bool:
        for v1, v2, desc in zip(self.values, other.values, self.descending_orders):
            equal = pc.equal(v1, v2)
            if equal.is_valid and self.SCALAR_TRUE.equals(equal):
                continue
            else:
                if desc:
                    greater = pc.greater(v1, v2)
                    return greater.is_valid and self.SCALAR_TRUE.equals(greater)
                else:
                    less = pc.less(v1, v2)
                    return less.is_valid and self.SCALAR_TRUE.equals(less)
        return False  # All keys are equal

    def __eq__(self, other: Self) -> bool:
        for v1, v2 in zip(self.values, other.values):
            equal = pc.equal(v1, v2)
            return equal.is_valid and self.SCALAR_TRUE.equals(equal)
