"""Query plan nodes that aggregations.

Frequently when analysing data is necessary
to compute statistics like the min, max, average, etc...
of the data stored in datasets.

The aggregate node is in charge of computing
those aggregations and projecting them as new
columns in a query pipeline.

Typically the aggregate node will group the data
by a set of columns and then compute the aggregations

For example, given the following data::

    city, shop, n_employees
    New York, Shop A, 10
    New York, Shop B, 15
    Los Angeles, Shop C, 8
    Los Angeles, Shop D, 12
    New York, Shop E, 20

We could group by city and compute the sum of the employees
to get::

    city, total_employees
    New York, 45
    Los Angeles, 20
"""

import abc
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .base import QueryPlanNode

__all__ = (
    "AggregateNode",
    "SumAggregation",
    "MinAggregation",
    "MaxAggregation",
    "MeanAggregation",
)


class AggregateNode(QueryPlanNode):
    """Group data and compute aggregations.

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> from datapyground.compute import col, lit, SumAggregation, PyArrowTableDataSource
    >>> data = pa.record_batch({
    ...    'city': pa.array(['New York', 'New York', 'Los Angeles', 'Los Angeles', 'New York']),
    ...    'shop': pa.array(['Shop A', 'Shop B', 'Shop C', 'Shop D', 'Shop E']),
    ...    'n_employees': pa.array([10, 15, 8, 12, 20])
    ... })
    >>> aggregate = AggregateNode(["city"], {"total_employees": SumAggregation("n_employees")}, PyArrowTableDataSource(data))
    >>> next(aggregate.batches())
    pyarrow.RecordBatch
    city: string
    total_employees: int64
    ----
    city: ["New York","Los Angeles"]
    total_employees: [45,20]
    """

    def __init__(
        self,
        keys: list[str],
        aggregations: dict[str, "Aggregation"],
        child: QueryPlanNode,
    ) -> None:
        """
        :param keys: The columns to group by.
        :param aggregations: The aggregations to compute in the form of {"new_col_name": Aggregation}.
        :param child: The child node that will provide the data to aggregate.
        """
        self.keys = keys
        self.aggregations = aggregations
        self.child = child

    def __str__(self) -> str:
        return f"AggregateNode(keys={self.keys}, aggregations={self.aggregations}, {self.child})"

    def batches(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Apply the filtering to the child node.

        For each recordbatch yielded by the child node,
        apply the expression and get back a mask
        (an array of only true/false values).

        Based on the mask filter the rows of the batch
        and return only those matching the filter.
        """
        if len(self.keys) == 1:
            yield from self.single_key_aggregation()
        else:
            yield from self.multi_key_aggregation()

    def single_key_aggregation(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Compute the aggregation for a single key.

        This is an optimized path where we can rely on dictionary encoding
        to find the unique values of the key column and then filter the rows.
        """
        # Compute separate aggregation results for each batch.
        # This makes so that we need to keep in memory only one batch
        # at the time, and the aggregation results, which are far smaller
        #   chunks_data = {key_value: {aggr_name: [aggr_value1, aggr_value2, ...]}}
        chunks_data: dict[pa.Scalar, dict[str, list[pa.Scalar]]] = {}
        for batch in self.child.batches():
            # Dictinary Encode the key variable,
            # so we can get the unique values
            # and we can know at which rows each value is.
            key_column = batch.column(self.keys[0])
            key_column = pc.dictionary_encode(key_column)
            key_values = key_column.dictionary
            key_indices = key_column.indices

            # For each unique value, we lookup the rows that have that value
            # Then for the resulting batch of rows filtered by the unique key value
            # we compute the aggregation and add it to the aggregation results for
            # that key value in the current batch.
            for idx, keyval in enumerate(key_values):
                chunks_data.setdefault(keyval, {})
                mask = pc.equal(key_indices, idx)
                filtered_batch = pc.filter(batch, mask)
                for name, aggregation in self.aggregations.items():
                    chunks_data[keyval].setdefault(name, []).append(
                        aggregation.compute_chunk(filtered_batch)
                    )

        # The chunks_data will contain the partial aggregation results for each key value
        # For example it could look like {"New York": {"total_employees": [10, 20, 30]}}
        # Now we need to reduce the partial aggregation results to get the final aggregation results
        # Which woud lead to {"New York": {"total_employees": 60}}
        yield self.reduce_aggregations(chunks_data)

    def multi_key_aggregation(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Compute the aggregation for multiple keys.

        In this case we will have to manually implement the grouping
        as we can't rely on dictionary encoding to find the unique values
        """
        # The most simple way to implement multi-key aggregation would be
        # to create a StructArray or ListArray out of the aggregation keys
        # And then dictionary encode that array to find the unique values.
        # But dictionary encoding is currently not supported for StructArray or ListArray
        #
        # Instead we will manually implement the aggregation in python,
        # it's much slower, but it shows how aggregation can be implemented.
        sorting_key = [(k, "ascending") for k in self.keys]
        chunks_data: dict[tuple[pa.Scalar], dict[str, list[pa.Scalar]]] = {}
        for batch in self.child.batches():
            # First sort the data by the aggregation keys,
            # this makes sure that we can compute the aggregation in a single pass.
            # All the values for the same grouping key will be sequential
            # For example:
            #    Los Angeles, Shop A, 8
            #    New York, Shop A, 10
            #    New York, Shop B, 20
            # so until the key changes we can compute the aggregation.
            # another alterantive would be to use a hash table to keep track of the
            # aggregation values for each key
            sorted_batch = batch.sort_by(sorting_key)
            current_key = None
            chunk_start = 0
            for row_index in range(sorted_batch.num_rows):
                row_key = tuple(sorted_batch.column(k)[row_index] for k in self.keys)
                if current_key is None:
                    current_key = row_key
                if row_key != current_key:
                    # the key has changed, this means we finished a chunk of
                    # rows with the same key, we can compute the aggregation for this chunk.
                    chunk_length = row_index - chunk_start
                    chunk = sorted_batch.slice(chunk_start, chunk_length)
                    chunks_data.setdefault(current_key, {})
                    for name, aggregation in self.aggregations.items():
                        chunks_data[current_key].setdefault(name, []).append(
                            aggregation.compute_chunk(chunk)
                        )
                    current_key = row_key
                    chunk_start = row_index

            # Compute the aggregation for the last chunk
            if current_key is not None:
                chunk = sorted_batch.slice(chunk_start, batch.num_rows - chunk_start)
                chunks_data.setdefault(current_key, {})
                for name, aggregation in self.aggregations.items():
                    chunks_data[current_key].setdefault(name, []).append(
                        aggregation.compute_chunk(chunk)
                    )

        yield self.reduce_aggregations(chunks_data)

    def reduce_aggregations(
        self, chunks_data: dict[Any, dict[str, list[pa.Scalar]]]
    ) -> pa.RecordBatch:
        """Reduce the partial aggregation results to the final aggregation results.

        Both single and multi key aggregation will end up computing the aggregations
        for each chunk separately, this method will reduce the partial aggregation
        results to the final aggregation results.

        For example if we had 3 chunks and the chunks_data is::

            {"New York": {"total_employees": [10, 20, 30]}}

        The result will be::

            {"New York": {"total_employees": 60}}
        """
        # Prepare one column for each key and aggregation
        result_batch_data: dict[str, list[pa.Scalar]] = {
            **{k: [] for k in self.keys},
            **{k: [] for k in self.aggregations.keys()},
        }
        # For each key value, invoke the reduce method of the aggregation/
        # In case of a single key
        #   keyvalue is "New York"
        #   aggregated_values is {"total_employees": [10, 20, 30]}
        # In case of multiple keys
        #   keyvalue is ("New York", "Shop A")
        #   aggregated_values is {"total_employees": [10, 20, 30]}
        for keyvalue, aggregated_values in chunks_data.items():
            if isinstance(keyvalue, tuple):
                # multiple aggregation keys
                for i, key in enumerate(self.keys):
                    result_batch_data[key].append(keyvalue[i])
            else:
                # single aggregation key
                result_batch_data[self.keys[0]].append(keyvalue)
            for aggrname, aggregation in self.aggregations.items():
                result_batch_data[aggrname].append(
                    aggregation.reduce(aggregated_values[aggrname])
                )

        # The result_batch_data is already formed in a way understood by pyarrow to create batches.
        # For example it could look like {"city": ["New York", "Los Angeles"], "total_employees": [60, 30]}
        return pa.record_batch(result_batch_data)


class Aggregation(abc.ABC):
    """Base class for aggregations.

    Every aggregation is expected to implement
    a method to compute any needed intermediate results
    on a single chunk of data and then provide a reduce method
    to combine the intermediate results into a final result.
    """

    def __init__(self, column: str) -> None:
        self.column = column

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.column})"

    __repr__ = __str__

    @abc.abstractmethod
    def compute_chunk(self, batch: pa.RecordBatch) -> Any: ...

    @abc.abstractmethod
    def reduce(self, chunks: list[pa.Array]) -> pa.Array: ...


class SimpleAggregation(Aggregation):
    """Provide a base implementation for simple aggregations like min,max,sum.

    Simple aggregations are those where the function applied to compute
    intermediate results for a single chunk of data is the same as the function
    applied to combine the intermediate results into the final.

    For example ``sum([1, 2, 3])`` is the same as ``sum([sum([1, 2]), 3])``.
    """

    def __init__(self, column: str) -> None:
        self.column = column

    @abc.abstractmethod
    def _aggregate(self, data: Any) -> Any: ...

    def compute_chunk(self, batch: pa.RecordBatch) -> Any:
        return self._aggregate(batch.column(self.column))

    def reduce(self, chunks: list[Any]) -> pa.Scalar:
        return self._aggregate(chunks)


class SumAggregation(SimpleAggregation):
    """Compute the sum of an aggregated column."""

    def _aggregate(self, data: Any) -> Any:
        return pc.sum(data)


class MinAggregation(SimpleAggregation):
    """Compute the min of an aggregated column."""

    def _aggregate(self, data: Any) -> Any:
        return pc.min(data)


class MaxAggregation(SimpleAggregation):
    """Compute the max of an aggregated column."""

    def _aggregate(self, data: Any) -> Any:
        return pc.max(data)


class CountAggregation(Aggregation):
    """Compute the count of an aggregated column.

    This is based on computing the counts for each intermediate batch
    and then sum them to compute the final result.
    """

    def compute_chunk(self, batch: pa.RecordBatch) -> Any:
        """Compute the count of the column in a single batch."""
        return pc.count(batch.column(self.column))

    def reduce(self, chunks: list[Any]) -> pa.Scalar:
        """Sum the counts of all intermediate results to the final count."""
        return pc.sum(chunks)


class MeanAggregation(Aggregation):
    """Compute the mean of an aggregated column.

    This is based by computing count and sum of the column
    for each intermediate batch and then dividing
    the sum of all intermediate results by the count
    of all intermediate results.
    """

    def compute_chunk(self, batch: pa.RecordBatch) -> Any:
        """Compute the count and sum of the column in a single batch."""
        col = batch.column(self.column)
        return (pc.count(col), pc.sum(col))

    def reduce(self, chunks: list[pa.Scalar]) -> pa.Array:
        """Compute the mean of the column from the intermediate sums and counts."""
        count = pc.sum([chunk[0] for chunk in chunks])
        total = pc.sum([chunk[1] for chunk in chunks])
        return pc.divide(total, count)
