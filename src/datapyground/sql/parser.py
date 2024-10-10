"""A basic SQL Parser for parsing SQL queries into Abstract Syntax Trees (AST).

Given a SQL Query like ``"SELECT id, name FROM users, orders WHERE users.id = orders.user_id"``,
the parser will produce an AST like::

    {
        "type": "select",
        "projections": [
            {"type": "identifier", "value": "id"},
            {"type": "identifier", "value": "name"},
        ],
        "from": [
            {"type": "identifier", "value": "users"},
            {"type": "identifier", "value": "orders"}
        ],
        "where": {
            "left": {"type": "identifier", "value": "users.id"},
            "op": {"type": "operator", "value": "="},
            "right": {"type": "identifier", "value": "orders.user_id"},
        },
        "group_by": None,
        "order_by": None,
        "limit": None,
        "offset": None,
    }

At the moment only SELECT statements are handled because DataPyground is a read-only platform.

The parser is based on a simple recursive descent parsing approach, where each SQL clause
is parsed by a dedicated method. The parser is implemented as a class with methods for each
clause, and the parsing is done by advancing through the tokens and building the AST.

The parser is not a full SQL parser and has limitations in terms of what it can support.
For example, it does not support nested queries, subqueries, or complex expressions.

The parser is designed to be simple and educational, to showcase how SQL queries can be parsed
and converted into an AST. In a production project, you would typically use a dedicated SQL parser
library like SQLGlot or Calcite.
"""

from .expressions import ExpressionParser, SQLExpressionError
from .tokenize import (
    AliasToken,
    EOFToken,
    FromToken,
    GroupByToken,
    IdentifierToken,
    InsertToken,
    JoinOnToken,
    JoinToken,
    JoinTypeToken,
    LimitToken,
    LiteralToken,
    OffsetToken,
    OrderByToken,
    PunctuationToken,
    SelectToken,
    SortingOrderToken,
    Token,
    Tokenizer,
    UpdateToken,
    WhereToken,
)


class Parser:
    """A simple SQL Parser for parsing SQL queries into Abstract Syntax Trees (AST).

    The Parser class identifies what type of query is being parsed and delegates the parsing
    to the appropriate parser for that query type. At the moment, only SELECT statements are supported
    and are handled by the :class:`SelectStatementParser` class.

    The parser relies on :class:`datapyground.sql.tokenize.Tokenizer` to tokenize the input SQL query
    and convert it into a list of :class:`datapyground.sql.tokenize.Token` objects that the
    parser will work with.
    """

    def __init__(self, text: str) -> None:
        """
        :param text: The input SQL query text to parse.
        """
        self.tokens = Tokenizer(text).tokenize()

    def parse(self) -> dict:
        """Parse the query and return the Abstract Syntax Tree (AST).

        The AST is always in the form of dictionary containing
        the structure of the parsed SQL query::

            {
                "type": "select",
                "projections": [...],
                "from": [...],
                "where": [{...}],
                "group_by": [...],
                "order_by": [...],
                "limit": ...,
                "offset": ...
            }
        """
        if not self.tokens:
            raise SQLParseError("Empty Query.")

        sql_command = self.tokens[0]
        if isinstance(sql_command, SelectToken):
            # Delegate parsing of SELECT statements to SelectStatementParser
            return SelectStatementParser(self.tokens).parse()
        elif isinstance(sql_command, InsertToken):
            # INSERT statements are recognized but not implemented
            raise NotImplementedError("INSERT statements are not supported yet.")
        elif isinstance(sql_command, UpdateToken):
            # UPDATE statements are recognized but not implemented
            raise NotImplementedError("UPDATE statements are not supported yet.")
        else:
            # Unsupported statement type encountered
            raise SQLParseError(f"Unsupported statement type: {sql_command.value}")


class SelectStatementParser:
    """A parser for SELECT statements that converts SQL queries into Abstract Syntax Trees (AST).

    The :class:`Parser` class delegates the parsing of SELECT statements to this class, which
    handles the parsing of SELECT statements only.

    The main parser has already tokenized the input , so this parser works directly with the tokens
    """

    def __init__(self, tokens: list[Token]) -> None:
        """
        :param tokens: A list of tokens representing the SQL query to parse.
        """
        self.tokens = tokens
        self.pos = 0
        self.current_token = tokens[self.pos]

    def advance(self, count: int = 1) -> None:
        """Advance the parser current_token to a subsequent token in the token list.

        By default it will move to the next token, as that's the
        most common use case.

        But when invoking subparsers like :class:`datapyground.sql.expressions.ExpressionParser`,
        it will be necessary to advance by as many tokens as the subparser consumed.
        """
        self.pos += count
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]
        else:
            self.current_token = EOFToken()

    def peek(self) -> Token:
        """Allows to take a look at what's the next token, without advancing the parser.

        In case the behavior of parsing the current token depends on the token that
        follows it, this function allows to check what's the next token without
        consuming it.

        After the parser has took a look at the subsequent token, and
        decided what to do with it, it will have to subsequently advance the parser
        to consume the token.
        """
        next_pos = self.pos + 1
        if next_pos < len(self.tokens):
            return self.tokens[next_pos]
        else:
            return EOFToken()

    def parse(self) -> dict:
        """Parse the SELECT statement and return the Abstract Syntax Tree (AST).

        In case any part of the select is missing,
        like ORDER BY, GROUP BY, LIMIT, OFFSET, the corresponding
        AST field will be present and set to None.

        The only required part are the SELECT projections and FROM clauses.
        """
        if not isinstance(self.current_token, SelectToken):
            raise SQLParseError(f"Expected SELECT statement, got: {self.current_token}")
        self.advance()  # Advance past 'SELECT'

        projections = self.parse_projections()

        if isinstance(self.current_token, FromToken):
            self.advance()  # Consume 'FROM' token
            from_clause = self.parse_from_clause()
        else:
            raise SQLParseError(
                f"Expected 'FROM' after projections, got: {self.current_token}"
            )

        where_clause = None
        if isinstance(self.current_token, WhereToken):
            self.advance()  # Consume 'WHERE'
            where_clause = self.parse_where_clause()

        group_by_clause = None
        if isinstance(self.current_token, GroupByToken):
            self.advance()  # Consume 'GROUP BY'
            group_by_clause = self.parse_group_by_clause()

        order_by_clause = None
        if isinstance(self.current_token, OrderByToken):
            self.advance()  # Consume 'ORDER BY'
            order_by_clause = self.parse_order_by_clause()

        limit_clause = None
        if isinstance(self.current_token, LimitToken):
            self.advance()  # Consume 'LIMIT'
            limit_clause = self.parse_limit_or_offset_clause()

        offset_clause = None
        if isinstance(self.current_token, OffsetToken):
            self.advance()  # Consume 'OFFSET'
            offset_clause = self.parse_limit_or_offset_clause()

        if not isinstance(self.current_token, EOFToken):
            raise SQLParseError(f"Unexpected token: {self.current_token}")

        ast = {
            "type": "select",
            "projections": projections,
            "from": from_clause,
            "where": where_clause,
            "group_by": group_by_clause,
            "order_by": order_by_clause,
            "limit": limit_clause,
            "offset": offset_clause,
        }

        return ast

    def parse_projections(self) -> list[dict]:
        """Parse the SELECT projections from the SQL query.

        Projections can be expressions too, like ``SUM(salary)``, or ``ROUND(SUM(salary), 2)``
        or even ``A + B``, handles aliases like ``SELECT SUM(salary) AS total_salary`` too.

        Returns a list of projection something like::

            {"type": "projection", "value": {"type": "identifier", "value": "id"}, "alias": "CustomerID"}
        """
        projections = []
        while True:
            projection = {
                "type": "projection",
                "value": self.parse_expression(),
                "alias": None,
            }
            if isinstance(self.current_token, AliasToken):
                self.advance()
                if isinstance(self.current_token, IdentifierToken):
                    projection["alias"] = self.current_token.value
                    self.advance()
            projections.append(projection)
            if not self.consume_punctuation(","):
                break
        return projections

    def parse_from_clause(self) -> list[dict]:
        """Parse the FROM clause of the SQL query.

        The FROM clause can contain multiple tables, separated by commas.

        Returns a list of table names.
        """
        tables = []
        while True:
            if isinstance(self.current_token, IdentifierToken):
                left_table = self.current_token.value
                self.advance()
                if isinstance(self.current_token, (JoinTypeToken, JoinToken)):
                    tables.append(self.parse_join_clause(left_table))
                else:
                    tables.append({"type": "identifier", "value": left_table})
            else:
                raise SQLParseError("Expected table name in FROM clause")
            if not self.consume_punctuation(","):
                break
        return tables

    def parse_join_clause(self, left_table: str) -> dict:
        """Parse a JOIN clause from the SQL query.

        A JOIN clause starts with the optional Join Type: INNER, LEFT, RIGHT, FULL, CROSS, NATURAL
        if missing, INNER JOIN is assumed.

        The JOIN clause is followed by the table name to join with, and optionally the ON keyword
        followed by an expression constituting the join condition.

        Returns a dictionary representing the join clause, looking like::

            {
                "type": "join",
                "join_type": "inner",
                "left_table": {"type": "identifier", "value": "users"},
                "right_table": {"type": "identifier", "value": "orders"},
                "join_condition": {
                    "left": {"type": "identifier", "value": "users.id"},
                    "op": {"type": "operator", "value": "="},
                    "right": {"type": "identifier", "value": "orders.user_id"},
                }
            }

        :param left_table: The name of the left table in the join clause.
        """
        # First we expect to find the optional join type:
        #  INNER, LEFT, RIGHT, FULL, CROSS, NATURAL
        join_type = []
        while isinstance(self.current_token, JoinTypeToken):
            join_type.append(self.current_token.value)
            self.advance()
        if not join_type:
            join_type = ["INNER"]

        # Next we expect to find the JOIN keyword
        if not isinstance(self.current_token, JoinToken):
            raise SQLParseError("Expected JOIN keyword in JOIN clause")
        self.advance()

        # Next we expect to find the right table name
        if not isinstance(self.current_token, IdentifierToken):
            raise SQLParseError("Expected table name in JOIN clause")
        right_table = self.current_token.value
        self.advance()

        # Next we expect to find the optional ON keyword
        if isinstance(self.current_token, JoinOnToken):
            self.advance()
            join_condition = self.parse_expression()
        else:
            join_condition = None

        return {
            "type": "join",
            "join_type": "_".join((k.lower() for k in join_type)),
            "left_table": {"type": "identifier", "value": left_table},
            "right_table": {"type": "identifier", "value": right_table},
            "join_condition": join_condition,
        }

    def parse_where_clause(self) -> dict:
        """Parse the WHERE clause of the SQL query.

        The WHERE clause is a conditional expression that filters the rows returned by the query.

        This method delegates the parsing of the expression to the ExpressionParser
        and it doesn't do much more than that. It expects the expression parser to
        return a boolean expression, but it doesn't enforce that.

        It's up to the consumer of the AST to interpret the WHERE clause correctly
        and eventually error if it's not a valid boolean expression.
        """
        return self.parse_expression()

    def parse_group_by_clause(self) -> list[dict]:
        """Parse the GROUP BY clause of the SQL query.

        Returns the list of columns to group by, looking like::

            [{"type": "identifier", "value": "id"}, {"type": "identifier", "value": "name"}]
        """
        group_by_columns = []
        while True:
            if isinstance(self.current_token, IdentifierToken):
                group_by_columns.append(
                    {"type": "identifier", "value": self.current_token.value}
                )
                self.advance()
            else:
                raise SQLParseError("Expected column name in GROUP BY clause")
            if not self.consume_punctuation(","):
                break
        return group_by_columns

    def parse_order_by_clause(self) -> list[dict]:
        """Parse the ORDER BY clause of the SQL query.

        Returns a list of columns to order by, with the sort order (ASC or DESC).

        The result will look like::

            [{"type": "ordering", "column": {"type": "identifier", "value": "id"}, "order": "ASC"},
             {"type": "ordering", "column": {"type": "identifier", "value": "name"}, "order": "DESC"}]
        """
        order_by_columns = []
        while True:
            if isinstance(self.current_token, IdentifierToken):
                column_name = self.current_token.value
                self.advance()
                sort_order = "ASC"  # Default sort order
                if isinstance(self.current_token, SortingOrderToken):
                    sort_order = self.current_token.value.upper()
                    self.advance()
                order_by_columns.append(
                    {
                        "type": "ordering",
                        "column": {"type": "identifier", "value": column_name},
                        "order": sort_order,
                    }
                )
            else:
                raise SQLParseError("Expected column name in ORDER BY clause")
            if not self.consume_punctuation(","):
                break
        return order_by_columns

    def parse_limit_or_offset_clause(self) -> int:
        """Parse the LIMIT or OFFSET clauses of the SQL query.

        Returns the value as an integer.
        """
        if isinstance(self.current_token, LiteralToken):
            limit_value = int(self.current_token.value)
            self.advance()
            return limit_value
        else:
            raise SQLParseError("Expected numeric literal after LIMIT or OFFSET")

    def parse_expression(self) -> dict:
        """Parse an expression from the SQL query.

        For the actual parsing it relies on the
        :class:`datapyground.sql.expressions.ExpressionParser`,
        after which it advances the parser by the number of tokens
        consumed by the expression parser.
        """
        try:
            offset, ast = ExpressionParser(self.tokens[self.pos :]).parse()
        except SQLExpressionError as e:
            raise SQLParseError(f"Error parsing expression: {e}")

        self.advance(offset)
        return ast

    def consume_punctuation(self, *values: str) -> bool:
        """Consume a punctuation token with a specific value.

        If the current token is a punctuation token with one of the specified values,
        it will consume the token and return True. Otherwise, it will return False.

        This is used by other parsing functions when there is a list of values to parse,
        it will consume the punctuation token slike ``','`` and return ``True`` as
        far as there are more values to consume.

        :param values: The list of valid punctuation characters to consume.
        """
        if (
            isinstance(self.current_token, PunctuationToken)
            and self.current_token.value in values
        ):
            self.advance()
            return True
        return False


class SQLParseError(Exception):
    """An exception raised when an error occurs during SQL parsing."""

    pass
