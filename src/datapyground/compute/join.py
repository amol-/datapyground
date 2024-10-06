"""Query plan nodes that implement join operations.

The join operations are implemented by aligning the keys of the two tables
and only keeping the rows where the keys are equal.

An alternative implementation would be to use a hash join algorithm
that builds a hash table from one of the tables and then probes the
other table to find matching rows.

Inner Join
==========

Provided by :class:`InnnerJoinNode`, the class provides a complete description
of the steps involved in performing an inner join operation.

>>> import pyarrow as pa
>>> from datapyground.compute import InnnerJoinNode
>>> from datapyground.compute import PyArrowTableDataSource
>>> left = PyArrowTableDataSource(pa.record_batch({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}))
>>> right = PyArrowTableDataSource(pa.record_batch({"id": [3, 2], "age": [25, 30]}))
>>> join_node = InnnerJoinNode("id", "id", left, right)
>>> next(join_node.batches())
pyarrow.RecordBatch
id: int64
name: string
age: int64
----
id: [2,3]
name: ["Bob","Charlie"]
age: [30,25]
"""

import pyarrow as pa
import pyarrow.compute as pc

from .base import QueryPlanNode


class InnnerJoinNode(QueryPlanNode):
    """Join two data sources using an inner join.

    The inner join is performed by aligning the keys of the two tables
    and only keeping the rows where the keys are equal.

    An alternative implementation would be to use a hash join algorithm
    that builds a hash table from one of the tables and then probes the
    other table to find matching rows. This is more efficient for large
    tables but PyArrow 0.17 doesn't currently expose a way to compute hashes
    and we want to avoid using Python loops to implement the hash join.

    Supposing we have two tables::

        left:
        +----+--------+
        | id | name   |
        +----+--------+
        | 1  | Alice  |
        | 2  | Bob    |
        | 3  | Charlie|
        +----+--------+


        right:
        +----+-----+
        | id | age |
        +----+-----+
        | 3  | 25  |
        | 2  | 30  |
        +----+-----+

    We would perform the following steps:

    1. Compute the unique values of the join keys in both tables.
       This is primarily done to speed up step 2::

        left_keys = [1, 2, 3]
        right_keys = [3, 2]

    2. Filter the tables to only keep the rows where the keys are in the other table.
       This allows us to remove the rows that don't have a match in the other table.
       As inner joins return only the rows that have a match in both tables::

        left:
        +----+--------+
        | id | name   |
        +----+--------+
        | 2  | Bob    |
        | 3  | Charlie|
        +----+--------+

        right:
        +----+-----+
        | id | age |
        +----+-----+
        | 3  | 25  |
        | 2  | 30  |
        +----+-----+

    3. Sort the tables so that the keys are aligned.
       This is necessary to make sure that to each key in the left table
       corresponds the same key in the right table. If the keys are not aligned
       the join operation wouldn't work as we would end up computing Bob is 25 years old.
       and Charlie is 30 years old. While instead according to the matching keys
       Bob should be 30 years old and Charlie should be 25 years old.
       After sorting the keys instead we seen that Bob is 30 years old and Charlie is 25 years old
       as row[0] has the same id in both tables and row[1] has the same id in both tables::

        left:
        +----+--------+
        | id | name   |
        +----+--------+
        | 2  | Bob    |
        | 3  | Charlie|
        +----+--------+

        right:
        +----+-----+
        | id | age |
        +----+-----+
        | 2  | 30  |
        | 3  | 25  |
        +----+-----+

    4. Combine the two tables into a new table.
       This is done by creating a new recordbatch that contains the columns of both tables.
       The keys are aligned so that for each key in the left table, the same key is in the right table
       at the same row. This way we can join on equal keys::

        combined:
        +----+--------+-----+
        | id | name   | age |
        +----+--------+-----+
        | 2  | Bob    | 30  |
        | 3  | Charlie| 25  |
        +----+--------+-----+

    """

    def __init__(
        self,
        left_key: str,
        right_key: str,
        left_child: QueryPlanNode,
        right_child: QueryPlanNode,
    ) -> None:
        """
        :param left_key: The key to join on in the left table.
        :param right_key: The key to join on in the right table.
        :param left_child: The left source of data to join.
        :param right_child: The right source of data to join.
        """
        self.left_key = left_key
        self.right_key = right_key
        self.left_child = left_child
        self.right_child = right_child

    def __str__(self) -> str:
        return f"InnerJoinNode(left_key={self.left_key}, right_key={self.right_key}, left={self.left_child}, right={self.right_child})"

    def batches(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Perform the inner join operation.

        Accumulates all rows of both children to
        perform the join operation, so it is not suitable
        for large datasets.
        """
        # To perform joins we need all rows in memory
        # so that we can compute unique values for the join keys
        # and align keys between the two tables.
        #
        # We currently need to go through tables as PyArrow 0.17
        # does not support concatenating batches.
        left_rb = (
            pa.Table.from_batches(self.left_child.batches())
            .combine_chunks()
            .to_batches()[0]
        )
        right_rb = (
            pa.Table.from_batches(self.right_child.batches())
            .combine_chunks()
            .to_batches()[0]
        )

        left_key = left_rb.column(self.left_key)
        right_key = right_rb.column(self.right_key)

        # Compute the unique values so that we can align the keys
        left_key_values = pc.unique(left_key)
        right_key_values = pc.unique(right_key)

        # Find which rows of each recordbatch match the other recordbatch
        # based on the join keys. This way the resulting recordbatches
        # will only contain rows that match the ones in the other dataset.
        #
        # As the left array is filtered to contain only rows that are in the right array
        # and the right array is filtered to contain only rows that are in the left array,
        # the resulting recordbatches will have the same number of rows.
        filtered_left_rb = left_rb.filter(
            pc.is_in(
                left_key,
                options=pc.SetLookupOptions(
                    value_set=right_key_values, skip_nulls=True
                ),
            )
        )
        filtered_right_rb = right_rb.filter(
            pc.is_in(
                right_key,
                options=pc.SetLookupOptions(value_set=left_key_values, skip_nulls=True),
            )
        )

        # Sort the recordbatches so that the keys are aligned,
        # this is necessary for the join operation to work as it
        # ensures that for each key in the left table, the same value
        # is in the right table at the same row. So we join on equal keys.
        sorted_left_rb = filtered_left_rb.take(
            pc.sort_indices(filtered_left_rb.column(self.left_key))
        )
        sorted_right_rb = filtered_right_rb.take(
            pc.sort_indices(filtered_right_rb.column(self.right_key))
        )

        # Combine the two record batches in a new one
        combined_data = {}
        for col in sorted_left_rb.column_names:
            combined_data[col] = sorted_left_rb.column(col)
        for col in sorted_right_rb.column_names:
            if col == self.right_key:
                # Skip the right key as it has the same values of the left key
                # and we don't want to duplicate it in the resulting recordbatch
                continue
            new_col_name = col
            if col in combined_data:
                # If the column already exists in the left table, we need to rename it
                new_col_name = col + "_right"
            combined_data[new_col_name] = sorted_right_rb.column(col)
        yield pa.record_batch(combined_data)
