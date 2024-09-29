import pytest

from datapyground.sql.expressions import ExpressionParser, SQLExpressionError
from datapyground.sql.tokenize import (
    Tokenizer,
)


def test_literal_expression():
    tokens = Tokenizer("42").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {"type": "literal", "value": 42}
    assert ast == expected_ast
    assert pos == len(tokens)


def test_identifier_expression():
    tokens = Tokenizer("age").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {"type": "identifier", "value": "age"}
    assert ast == expected_ast
    assert pos == len(tokens)


def test_unary_negation_expression():
    tokens = Tokenizer("-42").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "unary_op",
        "op": "-",
        "operand": {"type": "literal", "value": 42},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_binary_addition_expression():
    tokens = Tokenizer("1 + 2").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "binary_op",
        "op": "+",
        "left": {"type": "literal", "value": 1},
        "right": {"type": "literal", "value": 2},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_binary_multiplication_expression():
    tokens = Tokenizer("3 * 4").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "binary_op",
        "op": "*",
        "left": {"type": "literal", "value": 3},
        "right": {"type": "literal", "value": 4},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_comparison_expression():
    tokens = Tokenizer("age >= 18").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "comparison",
        "op": ">=",
        "left": {"type": "identifier", "value": "age"},
        "right": {"type": "literal", "value": 18},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_logical_and_expression():
    tokens = Tokenizer("age >= 18 AND age <= 65").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "conjunction",
        "op": "AND",
        "left": {
            "type": "comparison",
            "op": ">=",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 18},
        },
        "right": {
            "type": "comparison",
            "op": "<=",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 65},
        },
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_logical_or_expression():
    tokens = Tokenizer("age < 18 OR age > 65").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "conjunction",
        "op": "OR",
        "left": {
            "type": "comparison",
            "op": "<",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 18},
        },
        "right": {
            "type": "comparison",
            "op": ">",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 65},
        },
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_function_call_expression():
    tokens = Tokenizer("SUM(age)").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "function_call",
        "name": "SUM",
        "args": [{"type": "identifier", "value": "age"}],
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_nested_function_call_expression():
    tokens = Tokenizer("ROUND(SUM(salary), 2)").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "function_call",
        "name": "ROUND",
        "args": [
            {
                "type": "function_call",
                "name": "SUM",
                "args": [{"type": "identifier", "value": "salary"}],
            },
            {"type": "literal", "value": 2},
        ],
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_parenthesis_expression():
    tokens = Tokenizer("(age + 1) * 2").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "binary_op",
        "op": "*",
        "left": {
            "type": "binary_op",
            "op": "+",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 1},
        },
        "right": {"type": "literal", "value": 2},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_invalid_expression():
    tokens = Tokenizer("age +").tokenize()
    parser = ExpressionParser(tokens)
    with pytest.raises(SQLExpressionError) as excinfo:
        parser.parse()
    assert "Unexpected token" in str(excinfo.value)


def test_missing_closing_parenthesis():
    tokens = Tokenizer("(age + 1").tokenize()
    parser = ExpressionParser(tokens)
    with pytest.raises(SQLExpressionError) as excinfo:
        parser.parse()
    assert "Expected ')'" in str(excinfo.value)


def test_function_call_missing_closing_parenthesis():
    tokens = Tokenizer("SUM(age").tokenize()
    parser = ExpressionParser(tokens)
    with pytest.raises(SQLExpressionError) as excinfo:
        parser.parse()
    assert "Expected ')'" in str(excinfo.value)


def test_function_call_with_multiple_arguments():
    tokens = Tokenizer("CONCAT(first_name, ' ', last_name)").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "function_call",
        "name": "CONCAT",
        "args": [
            {"type": "identifier", "value": "first_name"},
            {"type": "literal", "value": " "},
            {"type": "identifier", "value": "last_name"},
        ],
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_nested_parenthesis_expression():
    tokens = Tokenizer("((age + 1) * 2)").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "binary_op",
        "op": "*",
        "left": {
            "type": "binary_op",
            "op": "+",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 1},
        },
        "right": {"type": "literal", "value": 2},
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_unary_not_expression():
    tokens = Tokenizer("NOT age >= 18").tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "unary_op",
        "op": "NOT",
        "operand": {
            "type": "comparison",
            "op": ">=",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 18},
        },
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_complex_expression():
    tokens = Tokenizer(
        "age >= 18 AND (salary > 50000 OR position = 'Manager')"
    ).tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "conjunction",
        "op": "AND",
        "left": {
            "type": "comparison",
            "op": ">=",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 18},
        },
        "right": {
            "type": "conjunction",
            "op": "OR",
            "left": {
                "type": "comparison",
                "op": ">",
                "left": {"type": "identifier", "value": "salary"},
                "right": {"type": "literal", "value": 50000},
            },
            "right": {
                "type": "comparison",
                "op": "=",
                "left": {"type": "identifier", "value": "position"},
                "right": {"type": "literal", "value": "Manager"},
            },
        },
    }
    assert ast == expected_ast
    assert pos == len(tokens)


def test_complex_expression_with_float():
    tokens = Tokenizer(
        "age >= 18.5 AND (salary > 50000.75 OR position = 'Manager')"
    ).tokenize()
    parser = ExpressionParser(tokens)
    pos, ast = parser.parse()
    expected_ast = {
        "type": "conjunction",
        "op": "AND",
        "left": {
            "type": "comparison",
            "op": ">=",
            "left": {"type": "identifier", "value": "age"},
            "right": {"type": "literal", "value": 18.5},
        },
        "right": {
            "type": "conjunction",
            "op": "OR",
            "left": {
                "type": "comparison",
                "op": ">",
                "left": {"type": "identifier", "value": "salary"},
                "right": {"type": "literal", "value": 50000.75},
            },
            "right": {
                "type": "comparison",
                "op": "=",
                "left": {"type": "identifier", "value": "position"},
                "right": {"type": "literal", "value": "Manager"},
            },
        },
    }
    assert ast == expected_ast
    assert pos == len(tokens)
