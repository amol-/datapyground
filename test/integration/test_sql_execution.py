import pyarrow as pa

from datapyground.sql.parser import Parser
from datapyground.sql.planner import SQLQueryPlanner


def test_sql_query_on_pyarrow_table():
    data = pa.record_batch({"id": [1, 2, 1, 2, 3], "age": [25, 30, 35, 40, 45]})

    # Expected results
    expected_data = pa.record_batch(
        {
            "id": [1, 2, 3],
            "count": [2, 2, 1],
            "average_age": [30, 35, 45],
            "adjusted_avg_age": [31, 36, 46],
        }
    )

    # Perform the SQL query using datapyground.sql.Planner
    sql = "SELECT id, COUNT(id) AS count, AVG(age) AS average_age, average_age + 1 AS adjusted_avg_age FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": data})
    plan = planner.plan()
    result_table = next(plan.batches())

    # Validate the results
    assert result_table.equals(expected_data)
