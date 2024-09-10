"""Query plan nodes that perform sorting of data.

When computing ranks or looking for most significant
values, it's often necessary to sort the data based
on one or more columns.

Sorted data can also benefit aggregations which
will be able to run faster as all the values that
constitute a group are in succession.

This module implements the sorting capabilities.
"""

import os
from tempfile import NamedTemporaryFile
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from .base import QueryPlanNode


class SortNode(QueryPlanNode):
    """Sort data based on one or more columns.

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

    _TEMPORARY_FILE_PREFIX = "datapyground_"

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
        mmaped_files = []
        mmaped_batches = []
        try:
            for batch in self.child.batches():
                sorted_batch = self._sort_batch(batch)
                with NamedTemporaryFile(
                    prefix=self._TEMPORARY_FILE_PREFIX, delete=False
                ) as batch_file:
                    batch_file_name = batch_file.name
                    temporay_files.append(batch_file_name)
                    with pa.ipc.RecordBatchFileWriter(
                        batch_file, batch.schema
                    ) as writer:
                        writer.write_batch(sorted_batch)

                mmaped_file = pa.memory_map(batch_file_name, "r")
                mmaped_files.append(mmaped_file)
                mmaped_batches.append(pa.ipc.open_file(mmaped_file).read_all())

            table = pa.concat_tables(mmaped_batches, promote_options="none")
            table = table.sort_by(self.sorting)
            for batch in table.to_batches():
                yield batch
        finally:
            for mmaped_file in mmaped_files:
                mmaped_file.close()
            for temp_file in temporay_files:
                os.unlink(temp_file)

    def _sort_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        sort_indices = pc.sort_indices(batch, self.sorting)
        return batch.take(sort_indices)
