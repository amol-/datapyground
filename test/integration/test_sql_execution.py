import pyarrow as pa

from datapyground.sql.parser import Parser
from datapyground.sql.planner import SQLQueryPlanner


def test_sql_query_on_pyarrow_table():
    data = pa.record_batch({"id": [1, 2, 1, 2, 3], "age": [25, 30, 35, 40, 45]})
    expected_data = {
        "id": [1, 2, 3],
        "count": [2, 2, 1],
        "average_age": [30, 35, 45],
        "adjusted_avg_age": [31, 36, 46],
    }

    sql = "SELECT id AS id, COUNT(id) AS count, AVG(age) AS average_age, average_age + 1 AS adjusted_avg_age FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": data})
    plan = planner.plan()
    result_table = next(plan.batches())

    assert result_table.to_pydict() == expected_data


def test_sql_query_with_join_and_filter():
    users_data = pa.record_batch({"id": [1, 2, 3], "age": [25, 30, 30]})
    orders_data = pa.record_batch(
        {"user_id": [1, 2, 3, 4], "amount": [100, 200, 150, 300]}
    )
    expected_data = {
        "users.age": [30],
        "total_amount": [350],
    }

    sql = "SELECT users.age, SUM(orders.amount) AS total_amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100 GROUP BY users.age"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(
        query, catalog={"users": users_data, "orders": orders_data}
    )
    plan = planner.plan()
    result_table = next(plan.batches())

    assert result_table.to_pydict() == expected_data
