import pyarrow as pa
import pyarrow.compute as pc
import pytest

from datapyground.compute import (
    FunctionCallExpression,
    PyArrowTableDataSource,
    col,
    lit,
)
from datapyground.compute.selection import ProjectNode


@pytest.fixture
def mock_data():
    """Create a mock PyArrow Table for testing."""
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    table = pa.table(data)
    return table


def test_init_and_str(mock_data):
    """Test the initialization and string representation of ProjectNode."""
    expressions = {"sum_ab": FunctionCallExpression(pc.add, col("a"), col("b"))}
    project_node = ProjectNode(
        ["a", "b"], expressions, PyArrowTableDataSource(mock_data)
    )
    assert (
        str(project_node)
        == "ProjectNode(select=['a', 'b'], project={'sum_ab': pyarrow.compute.add(ColumnRef(a),ColumnRef(b))}, child=PyArrowTableDataSource(columns=['a', 'b', 'c'], rows=3))"
    )


def test_select_columns(mock_data):
    """Test selecting specific columns."""
    project_node = ProjectNode(["a", "b"], {}, PyArrowTableDataSource(mock_data))
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 2
    assert batch.column_names == ["a", "b"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [4, 5, 6]


def test_project_columns(mock_data):
    """Test projecting new columns using expressions."""
    expressions = {"sum_ab": FunctionCallExpression(pc.add, col("a"), col("b"))}
    project_node = ProjectNode(["a"], expressions, PyArrowTableDataSource(mock_data))
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 2
    assert batch.column_names == ["a", "sum_ab"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [5, 7, 9]


def test_select_and_project_columns(mock_data):
    """Test selecting specific columns and projecting new columns."""
    expressions = {"sum_ab": FunctionCallExpression(pc.add, col("a"), col("b"))}
    project_node = ProjectNode(
        ["a", "c"], expressions, PyArrowTableDataSource(mock_data)
    )
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 3
    assert batch.column_names == ["a", "c", "sum_ab"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [7, 8, 9]
    assert batch.column(2).to_pylist() == [5, 7, 9]


def test_multiple_project_columns(mock_data):
    """Test projecting multiple new columns using expressions."""
    expressions = {
        "sum_ab": FunctionCallExpression(pc.add, col("a"), col("b")),
        "double_sum_ab": FunctionCallExpression(pc.multiply, col("sum_ab"), lit(2)),
    }
    project_node = ProjectNode(["a"], expressions, PyArrowTableDataSource(mock_data))
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 3
    assert batch.column_names == ["a", "sum_ab", "double_sum_ab"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [5, 7, 9]
    assert batch.column(2).to_pylist() == [10, 14, 18]


def test_project_column_not_selected(mock_data):
    """Test projecting a column that depends on a column that wasn't selected."""
    expressions = {"sum_bc": FunctionCallExpression(pc.add, col("b"), col("c"))}
    project_node = ProjectNode(["a"], expressions, PyArrowTableDataSource(mock_data))
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 2
    assert batch.column_names == ["a", "sum_bc"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [11, 13, 15]


def test_project_with_no_columns(mock_data):
    """Test projecting with no columns selected or projected."""
    project_node = ProjectNode([], {}, PyArrowTableDataSource(mock_data))
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 0


def test_project_with_all_columns(mock_data):
    """Test projecting with all columns selected."""
    expressions = {"sum_ab": FunctionCallExpression(pc.add, col("a"), col("b"))}
    project_node = ProjectNode(
        ["a", "b", "c"], expressions, PyArrowTableDataSource(mock_data)
    )
    batches = list(project_node.batches())
    assert len(batches) == 1
    batch = batches[0]
    assert batch.num_columns == 4
    assert batch.column_names == ["a", "b", "c", "sum_ab"]
    assert batch.column(0).to_pylist() == [1, 2, 3]
    assert batch.column(1).to_pylist() == [4, 5, 6]
    assert batch.column(2).to_pylist() == [7, 8, 9]
    assert batch.column(3).to_pylist() == [5, 7, 9]
