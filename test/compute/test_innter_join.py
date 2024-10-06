import pyarrow as pa
import pytest

from datapyground.compute import PyArrowTableDataSource
from datapyground.compute.join import InnnerJoinNode

# Sample data for testing
LEFT_TEST_DATA = pa.record_batch(
    {
        "id": pa.array([1, 2, 3, 4]),
        "name": pa.array(["Alice", "Bob", "Charlie", "David"]),
    }
)

RIGHT_TEST_DATA = pa.record_batch(
    {
        "id": pa.array([3, 4, 5, 6]),
        "age": pa.array([25, 30, 35, 40]),
    }
)


@pytest.fixture
def left_data_source():
    return PyArrowTableDataSource(LEFT_TEST_DATA)


@pytest.fixture
def right_data_source():
    return PyArrowTableDataSource(RIGHT_TEST_DATA)


@pytest.mark.parametrize(
    "left_key,right_key,expected_output",
    [
        (
            "id",
            "id",
            pa.record_batch(
                {
                    "id": pa.array([3, 4]),
                    "name": pa.array(["Charlie", "David"]),
                    "age": pa.array([25, 30]),
                }
            ),
        ),
    ],
)
def test_inner_join_node(
    left_data_source, right_data_source, left_key, right_key, expected_output
):
    join_node = InnnerJoinNode(left_key, right_key, left_data_source, right_data_source)
    result_batches = list(join_node.batches())

    assert len(result_batches) == 1
    result_batch = result_batches[0]

    assert result_batch.equals(expected_output)


def test_inner_join_node_conflicting_keys(left_data_source, right_data_source):
    left_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([1, 2, 3, 4]),
                "name": pa.array(["Alice", "Bob", "Charlie", "David"]),
                "conflict": pa.array(["A", "B", "C", "D"]),
            }
        )
    )
    right_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([3, 4, 5, 6]),
                "age": pa.array([25, 30, 35, 40]),
                "conflict": pa.array(["X", "Y", "Z", "W"]),
            }
        )
    )

    join_node = InnnerJoinNode("id", "id", left_data_source, right_data_source)
    result_batches = list(join_node.batches())

    assert len(result_batches) == 1
    result_batch = result_batches[0]

    assert set(result_batch.schema.names) == {
        "id",
        "name",
        "age",
        "conflict",
        "conflict_right",
    }


def test_inner_join_node_with_null_values():
    left_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([1, 2, None, 4]),
                "name": pa.array(["Alice", "Bob", "Charlie", "David"]),
            }
        )
    )
    right_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([3, 4, None, 6]),
                "age": pa.array([25, 30, 35, 40]),
            }
        )
    )

    join_node = InnnerJoinNode("id", "id", left_data_source, right_data_source)
    result_batches = list(join_node.batches())

    assert len(result_batches) == 1
    result_batch = result_batches[0]

    expected_output = pa.record_batch(
        {
            "id": pa.array([4]),
            "name": pa.array(["David"]),
            "age": pa.array([30]),
        }
    )

    assert result_batch.equals(expected_output)


def test_inner_join_node_with_nonexistent_keys():
    left_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([1, 2, 3, 4]),
                "name": pa.array(["Alice", "Bob", "Charlie", "David"]),
            }
        )
    )
    right_data_source = PyArrowTableDataSource(
        pa.record_batch(
            {
                "id": pa.array([5, 6, 7, 8]),
                "age": pa.array([25, 30, 35, 40]),
            }
        )
    )

    join_node = InnnerJoinNode("id", "id", left_data_source, right_data_source)
    result_batches = list(join_node.batches())

    assert len(result_batches) == 1
    result_batch = result_batches[0]
    assert result_batch.num_rows == 0


def test_inner_join_node_str(left_data_source, right_data_source):
    join_node = InnnerJoinNode("id", "id", left_data_source, right_data_source)
    assert (
        str(join_node)
        == "InnerJoinNode(left_key=id, right_key=id, left=PyArrowTableDataSource(columns=['id', 'name'], rows=4), right=PyArrowTableDataSource(columns=['id', 'age'], rows=4))"
    )
