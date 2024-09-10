import os

import pyarrow as pa
import pytest

from datapyground.compute.base import QueryPlanNode
from datapyground.compute.pagination import PaginateNode
from datapyground.compute.sorting import SortNode


class MockQueryPlanNode(QueryPlanNode):
    def __init__(self, batches):
        self._batches = batches

    def batches(self):
        for batch in self._batches:
            yield batch

    def __str__(self):
        return "MockQueryPlanNode"


def test_sort_node_single_batch():
    data = pa.record_batch({"values": [5, 3, 1, 4, 2]})
    child_node = MockQueryPlanNode([data])
    sort_node = SortNode(["values"], [False], child_node)

    sorted_batch = next(sort_node.batches())
    assert sorted_batch.column(0).to_pylist() == [1, 2, 3, 4, 5]


def test_sort_node_multiple_batches():
    data1 = pa.record_batch({"values": [5, 3]})
    data2 = pa.record_batch({"values": [1, 4, 2]})
    child_node = MockQueryPlanNode([data1, data2])
    sort_node = SortNode(["values"], [False], child_node)

    sorted_batches = list(sort_node.batches())
    sorted_values = [
        val for batch in sorted_batches for val in batch.column(0).to_pylist()
    ]
    assert sorted_values == [1, 2, 3, 4, 5]


def test_sort_node_descending():
    data = pa.record_batch({"values": [1, 2, 3, 4, 5]})
    child_node = MockQueryPlanNode([data])
    sort_node = SortNode(["values"], [True], child_node)

    sorted_batch = next(sort_node.batches())
    assert sorted_batch.column(0).to_pylist() == [5, 4, 3, 2, 1]


def test_sort_node_invalid_keys_and_descending_length():
    data = pa.record_batch({"values": [1, 2, 3, 4, 5]})
    child_node = MockQueryPlanNode([data])
    with pytest.raises(ValueError):
        SortNode(["values"], [True, False], child_node)


def test_sort_node_with_paginate_node():
    child_node = MockQueryPlanNode(
        [
            pa.record_batch({"values": [5, 3, 1, 4, 2]}),
            pa.record_batch({"values": [6, 9, 8, 7, 10]}),
        ]
    )
    sort_node = SortNode(["values"], [False], child_node)
    paginate_node = PaginateNode(offset=0, length=2, child=sort_node)

    sorted_batches = next(paginate_node.batches())
    assert sorted_batches["values"].to_pylist() == [1, 2]

    # Ensure temporary files are deleted
    temp_files = [
        f for f in os.listdir("/tmp") if f.startswith(SortNode._TEMPORARY_FILE_PREFIX)
    ]
    assert len(temp_files) == 0
