import pyarrow as pa

from datapyground.compute import PyArrowTableDataSource
from datapyground.compute.aggregate import (
    AggregateNode,
    CountAggregation,
    MaxAggregation,
    MeanAggregation,
    MinAggregation,
    SumAggregation,
)

TEST_DATA = pa.record_batch(
    {
        "city": pa.array(
            ["New York", "New York", "Los Angeles", "Los Angeles", "New York"]
        ),
        "shop": pa.array(["Shop A", "Shop B", "Shop C", "Shop D", "Shop E"]),
        "n_employees": pa.array([10, 15, 8, 12, 20]),
    }
)


def test_basic_aggregation():
    aggregate = AggregateNode(
        ["city"],
        {"total_employees": SumAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    assert result.column_names == ["city", "total_employees"]
    assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
    assert result.column(1).to_pylist() == [45, 20]


def test_aggregate_node_str():
    aggregate = AggregateNode(
        ["city"],
        {"total_employees": SumAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    assert str(aggregate) == (
        "AggregateNode(keys=['city'], aggregations={'total_employees': SumAggregation(n_employees)}, "
        "PyArrowTableDataSource(columns=['city', 'shop', 'n_employees'], rows=5))"
    )


def test_min_aggregation():
    aggregate = AggregateNode(
        ["city"],
        {"min_employees": MinAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    assert result.column_names == ["city", "min_employees"]
    assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
    assert result.column(1).to_pylist() == [10, 8]


def test_max_aggregation():
    aggregate = AggregateNode(
        ["city"],
        {"max_employees": MaxAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    assert result.column_names == ["city", "max_employees"]
    assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
    assert result.column(1).to_pylist() == [20, 12]


def test_count_aggregation():
    aggregate = AggregateNode(
        ["city"],
        {"count_employees": CountAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    assert result.column_names == ["city", "count_employees"]
    assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
    assert result.column(1).to_pylist() == [3, 2]


def test_mean_aggregation():
    aggregate = AggregateNode(
        ["city"],
        {"mean_employees": MeanAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    assert result.column_names == ["city", "mean_employees"]
    assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
    # Mean is calculated as sum / count
    assert result.column(1).to_pylist() == [15, 10]
