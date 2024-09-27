"""Support for SQL parsing and execution.

This module provides support for parsing and executing SQL queries
using the compute engine.

The platform is read-only so the only supported SQL queries are SELECT queries,
and the parser has limitations in terms of what it can support given that it
is based on a simple grammar and tokenization approach. On production projects,
you would typically use a dedicated library like SQLGlot or Calcite.
But given that DataPyground is a learning platform, the approach of implementing
a from scratch SQL parser is useful for educational purposes on the concept of parsing.

The SQL support is consiteuted by three components:

1. Tokenizer
2. Parser
3. Planner

The **Tokenizer** is responsible for converting the input SQL query into a sequence of tokens.
Given a query like ``"SELECT * FROM table WHERE column = 42"``, the tokenizer will produce
a sequence of tokens like::

    [SELECT, *, FROM, table, WHERE, column, =, 42]

The tokenizer implemented as part of DataPyground is a simple regex-based tokenizer.
It uses regular expressions to match which tokens exist within the input query.
This is a simple approach that has some limitations. For example, nested quotes
in string literals are not correctly supported.
But it's good enough for the purposes of showcasing how SQL queries can be parsed and executed.

The **Parser** is responsible for converting the sequence of tokens into an abstract syntax tree (AST).
In the case of DataPyground SQL parser, the AST is a simple nested dictionaries structure that represents the
structure of the query. For example, the query ``"SELECT id, name FROM users WHERE age >= 18 GROUP BY country"``
would be represented as an AST like::

    {
        'type': 'select',
        'projections': [
            {'type': 'column', 'value': 'id'},
            {'type': 'column', 'value': 'name'}
        ],
        'from': ['users'],
        'where': {
            'left': {'type': 'column', 'value': 'age'},
            'op': '>=',
            'right': {'type': 'literal', 'value': '18'}
        },
        'group_by': [
            {'type': 'column', 'value': 'country'}
        ]
    }

The **Planner** is responsible for taking the AST and generating a query plan
for the compute engine to execute the requested query. This is done by traversing
the AST and generating an equivalent tree of
:class:`datapyground.compute.base.QueryPlanNode` objects.
"""
