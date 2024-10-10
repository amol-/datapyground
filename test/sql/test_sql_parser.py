import pytest

from datapyground.sql.parser import Parser, SelectStatementParser, SQLParseError
from datapyground.sql.tokenize import EOFToken, IdentifierToken, Tokenizer


def test_select_query():
    query = "SELECT id, name FROM users WHERE age >= 18"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": {
            "type": "comparison",
            "left": {"type": "identifier", "value": "age"},
            "op": ">=",
            "right": {"type": "literal", "value": 18},
        },
        "group_by": None,
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_with_group_by():
    query = "SELECT id, name FROM users WHERE age >= 18 GROUP BY age"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": {
            "type": "comparison",
            "left": {"type": "identifier", "value": "age"},
            "op": ">=",
            "right": {"type": "literal", "value": 18},
        },
        "group_by": [{"type": "identifier", "value": "age"}],
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_without_where():
    query = "SELECT id as CustomerID, name FROM users"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": "CustomerID",
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": None,
        "group_by": None,
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_with_multiple_tables():
    query = "SELECT id, name FROM users, orders WHERE users.id = orders.user_id"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [
            {"type": "identifier", "value": "users"},
            {"type": "identifier", "value": "orders"},
        ],
        "where": {
            "type": "comparison",
            "left": {"type": "identifier", "value": "users.id"},
            "op": "=",
            "right": {"type": "identifier", "value": "orders.user_id"},
        },
        "group_by": None,
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_insert_query_not_implemented():
    query = "INSERT INTO users (id, name) VALUES (1, 'John')"
    parser = Parser(query)
    with pytest.raises(NotImplementedError) as excinfo:
        parser.parse()
    assert "INSERT statements are not supported yet." in str(excinfo.value)


def test_update_query_not_implemented():
    query = "UPDATE users SET name = 'John' WHERE id = 1"
    parser = Parser(query)
    with pytest.raises(NotImplementedError) as excinfo:
        parser.parse()
    assert "UPDATE statements are not supported yet." in str(excinfo.value)


def test_unsupported_statement():
    query = "DELETE FROM users WHERE id = 1"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Unsupported statement type: DELETE" in str(excinfo.value)


def test_empty_query():
    query = ""
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Empty Query." in str(excinfo.value)


def test_whitespace_query():
    query = "   "
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Empty Query." in str(excinfo.value)


def test_select_query_with_invalid_syntax():
    query = "SELECT id name FROM users"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected 'FROM' after projections, got: IdentifierToken('name')" in str(
        excinfo.value
    )


def test_select_query_with_missing_from():
    query = "SELECT id, name WHERE age >= 18"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected 'FROM' after projections" in str(excinfo.value)


def test_peek_method():
    query = "SELECT id, name FROM users WHERE age >= 18"
    parser = SelectStatementParser(Tokenizer(query).tokenize())

    # Skip tokens up to the FROM clause
    for _ in range(4):
        parser.advance()

    # Peek at the next token
    next_token = parser.peek()
    assert isinstance(next_token, IdentifierToken)
    assert next_token.value == "users"

    # Ensure peek handles EOF
    while parser.current_token != EOFToken():
        parser.advance()
    assert parser.peek() == EOFToken()


def test_select_query_with_order_by():
    query = "SELECT id, name FROM users ORDER BY age DESC"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": None,
        "group_by": None,
        "order_by": [
            {
                "type": "ordering",
                "column": {"type": "identifier", "value": "age"},
                "order": "DESC",
            }
        ],
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_with_limit():
    query = "SELECT id, name FROM users LIMIT 10"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": None,
        "group_by": None,
        "order_by": None,
        "limit": 10,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_with_offset():
    query = "SELECT id, name FROM users LIMIT 10 OFFSET 5"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": None,
        "group_by": None,
        "order_by": None,
        "limit": 10,
        "offset": 5,
    }

    assert ast == expected_ast


def test_select_query_with_all_clauses():
    query = "SELECT id, name FROM users WHERE age >= 18 GROUP BY age ORDER BY name ASC LIMIT 10 OFFSET 5"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "name"},
                "alias": None,
            },
        ],
        "from": [{"type": "identifier", "value": "users"}],
        "where": {
            "type": "comparison",
            "left": {"type": "identifier", "value": "age"},
            "op": ">=",
            "right": {"type": "literal", "value": 18},
        },
        "group_by": [{"type": "identifier", "value": "age"}],
        "order_by": [
            {
                "type": "ordering",
                "column": {"type": "identifier", "value": "name"},
                "order": "ASC",
            }
        ],
        "limit": 10,
        "offset": 5,
    }

    assert ast == expected_ast


def test_select_query_complex():
    query = "SELECT id, SUM(amount) AS Total FROM transactions WHERE (status = 'completed' AND date >= '2023-01-01') OR (status = 'pending' AND date < '2023-01-01') GROUP BY id"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {
                    "type": "function_call",
                    "name": "SUM",
                    "args": [{"type": "identifier", "value": "amount"}],
                },
                "alias": "Total",
            },
        ],
        "from": [{"type": "identifier", "value": "transactions"}],
        "where": {
            "type": "conjunction",
            "op": "OR",
            "left": {
                "type": "conjunction",
                "op": "AND",
                "left": {
                    "type": "comparison",
                    "left": {"type": "identifier", "value": "status"},
                    "op": "=",
                    "right": {"type": "literal", "value": "completed"},
                },
                "right": {
                    "type": "comparison",
                    "left": {"type": "identifier", "value": "date"},
                    "op": ">=",
                    "right": {"type": "literal", "value": "2023-01-01"},
                },
            },
            "right": {
                "type": "conjunction",
                "op": "AND",
                "left": {
                    "type": "comparison",
                    "left": {"type": "identifier", "value": "status"},
                    "op": "=",
                    "right": {"type": "literal", "value": "pending"},
                },
                "right": {
                    "type": "comparison",
                    "left": {"type": "identifier", "value": "date"},
                    "op": "<",
                    "right": {"type": "literal", "value": "2023-01-01"},
                },
            },
        },
        "group_by": [{"type": "identifier", "value": "id"}],
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast


def test_select_query_missing_from():
    query = "SELECT id, name WHERE age >= 18"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected 'FROM' after projections" in str(excinfo.value)


def test_select_query_missing_table_name():
    query = "SELECT id, name FROM WHERE age >= 18"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected table name in FROM clause" in str(excinfo.value)


def test_select_query_missing_where_condition():
    query = "SELECT id, name FROM users WHERE"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Error parsing expression: Empty expression." in str(excinfo.value)


def test_select_query_missing_group_by_column():
    query = "SELECT id, name FROM users GROUP BY"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected column name in GROUP BY clause" in str(excinfo.value)


def test_select_query_missing_order_by_column():
    query = "SELECT id, name FROM users ORDER BY"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected column name in ORDER BY clause" in str(excinfo.value)


def test_select_query_missing_limit_value():
    query = "SELECT id, name FROM users LIMIT"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected numeric literal after LIMIT or OFFSET" in str(excinfo.value)


def test_select_query_missing_offset_value():
    query = "SELECT id, name FROM users OFFSET"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected numeric literal after LIMIT or OFFSET" in str(excinfo.value)


def test_select_query_invalid_syntax():
    query = "SELECT id name FROM users"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected 'FROM' after projections, got: IdentifierToken('name')" in str(
        excinfo.value
    )


def test_not_select_query():
    query = "INSERT INTO users (id, name) VALUES (1, 'John')"
    parser = SelectStatementParser(Tokenizer(query).tokenize())
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Expected SELECT statement, got: InsertToken('INSERT')" in str(excinfo.value)


def test_select_query_invalid_operator():
    query = "SELECT id FROM users WHERE a OP c"
    parser = Parser(query)
    with pytest.raises(SQLParseError) as excinfo:
        parser.parse()
    assert "Unexpected token: IdentifierToken('OP')" in str(excinfo.value)


def test_select_query_with_simple_table_and_join():
    query = "SELECT users.id, users.name, orders.amount FROM unused, users JOIN orders ON users.id = orders.user_id"
    parser = Parser(query)
    ast = parser.parse()

    expected_ast = {
        "type": "select",
        "projections": [
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "users.id"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "users.name"},
                "alias": None,
            },
            {
                "type": "projection",
                "value": {"type": "identifier", "value": "orders.amount"},
                "alias": None,
            },
        ],
        "from": [
            {"type": "identifier", "value": "unused"},
            {
                "type": "join",
                "join_type": "inner",
                "left_table": {"type": "identifier", "value": "users"},
                "right_table": {"type": "identifier", "value": "orders"},
                "join_condition": {
                    "type": "comparison",
                    "left": {"type": "identifier", "value": "users.id"},
                    "op": "=",
                    "right": {"type": "identifier", "value": "orders.user_id"},
                },
            },
        ],
        "where": None,
        "group_by": None,
        "order_by": None,
        "limit": None,
        "offset": None,
    }

    assert ast == expected_ast
