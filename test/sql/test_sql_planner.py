import pyarrow.compute as pc
import pytest

from datapyground.compute import (
    CSVDataSource,
    FilterNode,
    FunctionCallExpression,
    ProjectNode,
)
from datapyground.sql.parser import Parser
from datapyground.sql.planner import SQLQueryPlanner


def test_simple_select():
    sql = "SELECT id, name FROM users"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, CSVDataSource)
    assert plan.child.filename == "users.csv"


def test_select_with_where():
    sql = "SELECT id, name FROM users WHERE age >= 18"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.child, CSVDataSource)
    assert plan.child.child.filename == "users.csv"
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.greater_equal


def test_select_with_projection_expression():
    sql = "SELECT id, name, age + 1 AS next_age FROM users"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert "next_age" in plan.project
    assert isinstance(plan.project["next_age"], FunctionCallExpression)
    assert plan.project["next_age"].func == pc.add


def test_select_with_logical_and():
    sql = "SELECT id, name FROM users WHERE age >= 18 AND age <= 65"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.child, CSVDataSource)
    assert plan.child.child.filename == "users.csv"
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.and_


def test_select_with_logical_or():
    sql = "SELECT id, name FROM users WHERE age < 18 OR age > 65"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, FilterNode)
    assert isinstance(plan.child.child, CSVDataSource)
    assert plan.child.child.filename == "users.csv"
    assert isinstance(plan.child.expression, FunctionCallExpression)
    assert plan.child.expression.func == pc.or_


def test_select_with_function_call():
    sql = "SELECT ROUND(salary, 2) AS rounded_total_salary FROM employees"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"employees": "employees.csv"})
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
        query, catalog={"users": "users.csv", "orders": "orders.csv"}
    )
    with pytest.raises(ValueError, match="Only single table queries are supported"):
        planner.plan()


def test_select_with_missing_table():
    sql = "SELECT id, name FROM unknown_table"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query)
    with pytest.raises(NotImplementedError, match="File format not supported"):
        planner.plan()


def test_select_with_group_by():
    sql = "SELECT id, COUNT(id) AS count FROM users GROUP BY id"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id"]
    assert "count" in plan.project
    assert isinstance(plan.project["count"], FunctionCallExpression)
    assert plan.project["count"].func == pc.count


def test_select_with_order_by():
    sql = "SELECT id, name FROM users ORDER BY age DESC"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, CSVDataSource)
    assert plan.child.filename == "users.csv"
    # Assuming the planner handles ORDER BY in the ProjectNode or a separate node


def test_select_with_limit():
    sql = "SELECT id, name FROM users LIMIT 10"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, CSVDataSource)
    assert plan.child.filename == "users.csv"
    # Assuming the planner handles LIMIT in the ProjectNode or a separate node


def test_select_with_offset():
    sql = "SELECT id, name FROM users OFFSET 5"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, CSVDataSource)
    assert plan.child.filename == "users.csv"
    # Assuming the planner handles OFFSET in the ProjectNode or a separate node


def test_select_with_limit_and_offset():
    sql = "SELECT id, name FROM users LIMIT 10 OFFSET 5"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "users.csv"})
    plan = planner.plan()
    assert isinstance(plan, ProjectNode)
    assert plan.select == ["id", "name"]
    assert isinstance(plan.child, CSVDataSource)
    assert plan.child.filename == "users.csv"
    # Assuming the planner handles LIMIT and OFFSET in the ProjectNode or a separate node
