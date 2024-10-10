import pyarrow as pa
import pyarrow.compute as pc
import pytest

from datapyground.compute import (
    AggregateNode,
    ColumnRef,
    FilterNode,
    FunctionCallExpression,
    InnnerJoinNode,
    PaginateNode,
    ProjectNode,
    PyArrowTableDataSource,
    SortNode,
)
from datapyground.compute import aggregate as agg
from datapyground.sql.parser import Parser
from datapyground.sql.planner import SQLQueryPlanner

users_table = pa.record_batch(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
)
employees_table = pa.record_batch(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "salary": [1000, 2000, 3000]}
)
orders_table = pa.record_batch(
    {"id": [1, 2, 3], "user_id": [1, 2, 3], "total": [100, 200, 300]}
)


def test_simple_select():
    sql = "SELECT id, name FROM users"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "users.name"]
    assert isinstance(plan.child, ProjectNode)
    assert plan.child.select == []
    assert plan.child.project == {
        "users.id": ColumnRef("id"),
        "users.name": ColumnRef("name"),
        "users.age": ColumnRef("age"),
    }
    assert isinstance(plan.child.child, PyArrowTableDataSource)
    assert plan.child.child.table == users_table


def test_select_with_where():
    sql = "SELECT id, name FROM users WHERE age >= 18"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "users.name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.greater_equal


def test_select_with_projection_expression():
    sql = "SELECT id, name, age + 1 AS next_age FROM users"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "users.name"]
    assert "next_age" in plan.project
    assert isinstance(plan.project["next_age"], FunctionCallExpression)
    assert plan.project["next_age"].func == pc.add


def test_select_with_logical_and():
    sql = "SELECT id, name FROM users WHERE age >= 18 AND age <= 65"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "users.name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.and_


def test_select_with_logical_or():
    sql = "SELECT id, name FROM users WHERE age < 18 OR age > 65"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "users.name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.or_


def test_select_with_function_call():
    sql = "SELECT ROUND(salary, 2) AS rounded_total_salary FROM employees"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"employees": employees_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert "rounded_total_salary" in plan.project
    assert isinstance(plan.project["rounded_total_salary"], FunctionCallExpression)
    assert plan.project["rounded_total_salary"].func == pc.round


def test_select_with_multiple_tables():
    sql = (
        "SELECT users.id, orders.id FROM users, orders WHERE users.id = orders.user_id"
    )
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(
        query, catalog={"users": users_table, "orders": orders_table}
    )
    with pytest.raises(ValueError, match="Only single table queries are supported"):
        planner.plan()


def test_select_with_missing_table():
    sql = "SELECT id, name FROM unknown_table"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query)
    with pytest.raises(NotImplementedError, match="File format not supported"):
        planner.plan()


def test_select_with_order_by():
    sql = "SELECT id, name FROM users ORDER BY age DESC"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, SortNode)
    assert plan.sorting == [("users.age", "descending")]
    assert isinstance(plan.child, ProjectNode)
    assert plan.child.select == ["users.id", "users.name"]


def test_select_with_limit():
    sql = "SELECT id, name FROM users LIMIT 10"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, PaginateNode)
    assert plan.length == 10
    assert plan.offset == 0
    assert isinstance(plan.child, ProjectNode)
    assert plan.child.select == ["users.id", "users.name"]


def test_select_with_offset():
    sql = "SELECT id, name FROM users OFFSET 5"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, PaginateNode)
    assert plan.length == PaginateNode.INF
    assert plan.offset == 5
    assert isinstance(plan.child, ProjectNode)
    assert plan.child.select == ["users.id", "users.name"]


def test_select_with_limit_and_offset():
    sql = "SELECT id, name FROM users LIMIT 10 OFFSET 5"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, PaginateNode)
    assert plan.length == 10
    assert plan.offset == 5
    assert isinstance(plan.child, ProjectNode)
    assert plan.child.select == ["users.id", "users.name"]


def test_select_with_group_by_count():
    sql = "SELECT id, COUNT(id) AS count FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "count"]
    assert isinstance(plan.child, AggregateNode)
    assert plan.child.keys == ["users.id"]
    assert "count" in plan.child.aggregations
    assert isinstance(plan.child.aggregations["count"], agg.CountAggregation)


def test_select_with_group_by_avg():
    sql = "SELECT id, AVG(age) AS average_age FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "average_age"]
    assert isinstance(plan.child, AggregateNode)
    assert plan.child.keys == ["users.id"]
    assert "average_age" in plan.child.aggregations
    assert isinstance(plan.child.aggregations["average_age"], agg.MeanAggregation)


def test_select_with_group_by_multiple_aggregations():
    sql = (
        "SELECT id, COUNT(id) AS count, AVG(age) AS average_age FROM users GROUP BY id"
    )
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "count", "average_age"]
    assert isinstance(plan.child, AggregateNode)
    assert plan.child.keys == ["users.id"]
    assert "average_age" in plan.child.aggregations
    assert "count" in plan.child.aggregations
    assert isinstance(plan.child.aggregations["average_age"], agg.MeanAggregation)
    assert isinstance(plan.child.aggregations["count"], agg.CountAggregation)


def test_select_with_group_by_and_projection():
    sql = "SELECT id, COUNT(id) AS count, AVG(age) AS average_age, average_age + 1 AS adjusted_avg_age FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": users_table})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "count", "average_age"]
    assert "adjusted_avg_age" in plan.project
    assert isinstance(plan.child, AggregateNode)
    assert plan.child.keys == ["users.id"]
    assert "average_age" in plan.child.aggregations
    assert "count" in plan.child.aggregations
    assert isinstance(plan.child.aggregations["average_age"], agg.MeanAggregation)
    assert isinstance(plan.child.aggregations["count"], agg.CountAggregation)


def test_select_with_join_condition():
    sql = "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id WHERE age >= 18"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(
        query, catalog={"users": users_table, "orders": orders_table}
    )
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["users.id", "orders.id"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.child, InnnerJoinNode)
    assert plan.child.child.left_key == "users.id"
    assert plan.child.child.right_key == "orders.user_id"
    assert isinstance(plan.child.child.left_child, ProjectNode)
    assert isinstance(plan.child.child.left_child.child, PyArrowTableDataSource)
    assert plan.child.child.left_child.child.table == users_table
    assert isinstance(plan.child.child.right_child, ProjectNode)
    assert isinstance(plan.child.child.right_child.child, PyArrowTableDataSource)
    assert plan.child.child.right_child.child.table == orders_table
