import pyarrow as pa
import pytest

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
        "shop": pa.array(["Shop A", "Shop B", "Shop A", "Shop A2", "Shop B"]),
        "n_employees": pa.array([10, 15, 8, 12, 20]),
    }
)


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_basic_aggregation(keys):
    aggregate = AggregateNode(
        keys,
        {"total_employees": SumAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "total_employees"]
        assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
        assert result.column(1).to_pylist() == [45, 20]
    else:
        assert result.column_names == ["city", "shop", "total_employees"]
        assert result.column(0).to_pylist() == [
            "Los Angeles",
            "Los Angeles",
            "New York",
            "New York",
        ]
        assert result.column(1).to_pylist() == ["Shop A", "Shop A2", "Shop A", "Shop B"]
        assert result.column(2).to_pylist() == [8, 12, 10, 35]


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_aggregate_node_str(keys):
    aggregate = AggregateNode(
        keys,
        {"total_employees": SumAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    assert str(aggregate) == (
        "AggregateNode(keys=%r, aggregations={'total_employees': SumAggregation(n_employees)}, "
        "PyArrowTableDataSource(columns=['city', 'shop', 'n_employees'], rows=5))"
        % (keys,)
    )


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_min_aggregation(keys):
    aggregate = AggregateNode(
        keys,
        {"min_employees": MinAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "min_employees"]
        assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
        assert result.column(1).to_pylist() == [10, 8]
    else:
        assert result.column_names == ["city", "shop", "min_employees"]
        assert result.column(0).to_pylist() == [
            "Los Angeles",
            "Los Angeles",
            "New York",
            "New York",
        ]
        assert result.column(1).to_pylist() == ["Shop A", "Shop A2", "Shop A", "Shop B"]
        assert result.column(2).to_pylist() == [8, 12, 10, 15]


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_max_aggregation(keys):
    aggregate = AggregateNode(
        keys,
        {"max_employees": MaxAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "max_employees"]
        assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
        assert result.column(1).to_pylist() == [20, 12]
    else:
        assert result.column_names == ["city", "shop", "max_employees"]
        assert result.column(0).to_pylist() == [
            "Los Angeles",
            "Los Angeles",
            "New York",
            "New York",
        ]
        assert result.column(1).to_pylist() == ["Shop A", "Shop A2", "Shop A", "Shop B"]
        assert result.column(2).to_pylist() == [8, 12, 10, 20]


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_count_aggregation(keys):
    aggregate = AggregateNode(
        keys,
        {"count_employees": CountAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "count_employees"]
        assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
        assert result.column(1).to_pylist() == [3, 2]
    else:
        assert result.column_names == ["city", "shop", "count_employees"]
        assert result.column(0).to_pylist() == [
            "Los Angeles",
            "Los Angeles",
            "New York",
            "New York",
        ]
        assert result.column(1).to_pylist() == ["Shop A", "Shop A2", "Shop A", "Shop B"]
        assert result.column(2).to_pylist() == [1, 1, 1, 2]


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_mean_aggregation(keys):
    aggregate = AggregateNode(
        keys,
        {"mean_employees": MeanAggregation("n_employees")},
        PyArrowTableDataSource(TEST_DATA),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "mean_employees"]
        assert result.column(0).to_pylist() == ["New York", "Los Angeles"]
        assert result.column(1).to_pylist() == [15, 10]
    else:
        assert result.column_names == ["city", "shop", "mean_employees"]
        assert result.column(0).to_pylist() == [
            "Los Angeles",
            "Los Angeles",
            "New York",
            "New York",
        ]
        assert result.column(1).to_pylist() == ["Shop A", "Shop A2", "Shop A", "Shop B"]
        assert result.column(2).to_pylist() == [8, 12, 10, 17]


@pytest.mark.parametrize("keys", [["city"], ["city", "shop"]])
def test_count_aggregation_50_rows(keys):
    aggregate = AggregateNode(
        keys,
        {"count_employees": CountAggregation("n_employees")},
        PyArrowTableDataSource(_generate_50rows_test_data()),
    )
    result = next(aggregate.batches())

    if keys == ["city"]:
        assert result.column_names == ["city", "count_employees"]
        assert result.column(0).to_pylist() == [
            "City0",
            "City1",
            "City2",
            "City3",
            "City4",
        ]
        assert result.column(1).to_pylist() == [20, 20, 20, 20, 20]
    else:
        assert result.column_names == ["city", "shop", "count_employees"]
        expected_cities = ["City" + str(i) for i in range(5) for _ in range(10)]
        expected_shops = ["Shop" + str(i) for _ in range(5) for i in range(10)]
        expected_counts = [2] * 50
        assert result.column(0).to_pylist() == expected_cities
        assert result.column(1).to_pylist() == expected_shops
        assert result.column(2).to_pylist() == expected_counts


def _generate_50rows_test_data():
    cities = ["City" + str(i) for i in range(5)]
    shops = ["Shop" + str(i) for i in range(10)]
    data = {"city": [], "shop": [], "n_employees": []}
    for city in cities:
        for shop in shops:
            for _ in range(2):  # Ensure each combination appears at least twice
                data["city"].append(city)
                data["shop"].append(shop)
                data["n_employees"].append(10)  # Arbitrary number of employees
    return pa.record_batch(data)
