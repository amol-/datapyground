"""Support for SQL parsing and query plans generation.

This module provides support for parsing and executing SQL queries
using the compute engine.

The platform is read-only so the only supported SQL queries are SELECT queries,
and the parser has limitations in terms of what it can support given that it
is based on a simple grammar and tokenization approach. On production projects,
you would typically use a dedicated library like SQLGlot or Calcite.
But given that DataPyground is a learning platform, the approach of implementing
a from scratch SQL parser is useful for educational purposes on the concept of parsing.

The SQL support is constituted by three major components:

1. Tokenizer
2. Parser (and ExpressionParser)
3. Planner

To execute a SQL query, you would typically combine them as following::

    sql = "SELECT id, name FROM users WHERE age >= 18"
    query = Parser(sql).parse()
    planner = SQLQueryPlanner(query, catalog={"users": "file.csv"})
    queryplan = planner.plan()

The `queryplan` object would be a tree of nodes that can be executed by the compute engine
to produce the result of the query. See the :mod:`datapyground.compute` module for more details.

The **Tokenizer** is responsible for converting the input SQL query into a sequence of tokens.
Given a query like ``"SELECT * FROM table WHERE column = 42"``, the tokenizer will produce
a sequence of tokens like::

    [SELECT, *, FROM, table, WHERE, column, =, 42]

The :class:`datapyground.sql.tokenize.Tokenizer` implemented as part of DataPyground is a simple regex-based tokenizer.
It uses regular expressions to match which tokens exist within the input query.
This is a simple approach that has some limitations. For example, nested quotes
in string literals are not correctly supported.
But it's good enough for the purposes of showcasing how SQL queries can be parsed and executed.

The :class:`datapyground.sql.expressions.ExpressionParser`
is responsible for handling a sequence of tokens representing expressions.
It is used by the main parser to parse expressions like ``age >= 18 AND city = "New York``.
The main parser will delegate work to it everytime it finds an expression.

The :class:`datapyground.sql.parser.Parser` is **the main class of the parser**,
in charge of converting a text query abstract syntax tree (AST).
In the case of DataPyground SQL parser, the AST is a simple nested dictionaries structure that represents the
structure of the query. For example, the query ``"SELECT id, name FROM users WHERE age >= 18 GROUP BY country"``
would be represented as an AST like::

    {
        'type': 'select',
        'projections': [
            {'type': 'identifier', 'value': 'id'},
            {'type': 'identifier', 'value': 'name'}
        ],
        'from': [{"type": "identifier", "value": "users"}],
        'where': {
            'left': {'type': 'identifier', 'value': 'age'},
            'op': '>=',
            'right': {'type': 'literal', 'value': '18'}
        },
        'group_by': [
            {'type': 'identifier', 'value': 'country'}
        ]
    }

The :class:`datapyground.sql.planner.SQLQueryPlanner` is responsible for
taking the AST and generating a query plan for the compute engine to execute
the requested query.
This is done by traversing the AST and generating an equivalent tree of
:class:`datapyground.compute.base.QueryPlanNode` objects.
"""

from .expressions import SQLExpressionError
from .parser import Parser, SQLParseError
from .planner import SQLQueryPlanner

__all__ = ("Parser", "SQLQueryPlanner", "SQLParseError", "SQLExpressionError")
