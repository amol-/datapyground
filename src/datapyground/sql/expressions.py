"""Implement a parser for SQL expressions.

An SQL expression is a combination of literals, identifiers, operators, and functions that
can be evaluated to a value. This module provides a parser for SQL expressions that can
parse expressions with the following features:

- Arithmetic operators: ``+, -, *, /``
- Comparison operators: ``=, <, >, <=, >=, <>, !=``
- Logical operators: ``AND, OR, NOT``
- Parentheses for grouping
- Function calls with arguments
- Identifiers and literals

The parser returns an abstract syntax tree (AST) that represents the expression,
in the same format as the AST returned by :class:`datapyground.sql.parser.Parser`.

The parser is implemented as a recursive descent parser, with each method in the
expression parser class corresponding to a different level of the grammar. The parser
advances through the tokens and builds the AST by recursively calling the appropriate
methods based on the current token.

In case of an expression like ``a + b * c != 3 AND NOT d``, the workflow would proceed as follows::

    - parse_expression (``a + b * c != 3 AND NOT d``)            # Handles OR last because it is the lowest precedence comparator
        - parse_term (``AND``)                                   # Handles AND first because it has an higher precedence comparator
            - parse_factor (``a + b * c != 3``)                  # Handles operands connected by AND
                - parse_comparison (``=``)                       # Handles comparison operators
                    - parse_additive_expr (``a + b * c``)        # Handles addition last as they have lower math precedence
                        - parse_multiplicative_expr (``b * c``)  # Handles multiplication first as they have higher math precedence
                            - parse_unary_expr (``b``)           # Handles possible -X to negate values
                                - parse_primary (``b``)          # Handles possible function calls wrapping an atom
                                    - parse_atom (``b``)         # Handles identifiers and literals
                            - parse_unary_expr (``c``)
                                - parse_primary (``c``)
                                    - parse_atom (``c``)
                        - parse_multiplicative_expr (``a``)
                            - parse_unary_expr (``a``)
                                - parse_primary (``a``)
                                    - parse_atom (``a``)
                - parse_additive_expr (``3``)                    # Handles the right side of the comparison, eventually processes the addition
                    - parse_multiplicative_expr (``3``)
                        - parse_unary_expr (``3``)
                            - parse_primary (``3``)
                                - parse_atom (``3``)
            - parse_factor (``NOT d``)                           # Handles the right side of the AND, processes NOT operator
                - parse_unary_expr (``NOT d``)
                    - parse_primary (``d``)
                        - parse_atom (``d``)

The resulting AST would look like this::

    {
        "type": "conjunction",
        "op": "AND",
        "left": {
            "type": "comparison",
            "op": "=",
            "left": {
                "type": "binary_op",
                "op": "+",
                "left": {"type": "identifier", "value": "a"},
                "right": {
                    "type": "binary_op",
                    "op": "*",
                    "left": {"type": "identifier", "value": "b"},
                    "right": {"type": "identifier", "value": "c"}
                }
            },
            "right": {
                "type": "unary_op",
                "op": "-",
                "operand": {"type": "literal", "value": 3}
            }
        },
        "right": {
            "type": "unary_op",
            "op": "NOT",
            "operand": {"type": "identifier", "value": "d"}
        }
    }
"""

from .tokenize import (
    EOFToken,
    IdentifierToken,
    LiteralToken,
    OperatorToken,
    PunctuationToken,
    Token,
)


class ExpressionParser:
    """A parser for SQL expressions.

    Handles parsing of SQL expressions like "a + b", "x > 5 AND y < 7" or "SUM(x) * 2".
    into an abstract syntax tree (AST).

    It is used by :class:`datapyground.sql.parser.Parser` to handle
    WHERE conditions and SELECT projection expressions.
    """

    def __init__(self, tokens: list[Token]) -> None:
        """
        :param tokens: A list of tokens representing the expression.
        """
        if not tokens:
            raise SQLExpressionError("Empty expression.")
        self.tokens = tokens
        self.pos = 0  # Current position in the tokens list
        self.current_token = tokens[self.pos]

    def advance(self) -> None:
        """Advance the parser to the next token.

        The parser keeps track of the current token that
        has to parse, this function is used to move to the next
        token after the current one has been parsed.
        """
        self.pos += 1
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]
        else:
            self.current_token = EOFToken()  # End of input

    def parse(self) -> tuple[int, dict]:
        """Main method to parse a whole expression.

        Returns the abstract syntax tree (AST) for the parsed expression
        and how many tokens were consumed to parse the expression.

        The amount of tokens is returned to allow the caller to know
        where the expression parsing ended and continue parsing other
        parts of the query.
        """
        ast = self.parse_expression()
        return self.pos, ast

    def parse_expression(self) -> dict:
        """Parse an expression, which are terms connected by OR

        Parses the left term of the expression and then
        if there is an OR it will parse the right term too.

        If there is no OR, it will return the left term as is.
        """
        term = self.parse_term()
        while self.is_operator("OR"):
            op = self.current_token.value.upper()
            self.advance()
            right = self.parse_term()
            term = {"type": "conjunction", "op": op, "left": term, "right": right}
        return term

    def parse_term(self) -> dict:
        """Parse a term, which are factors connected by AND.

        Parses the left factor of the expression and then
        if there is an AND it will parse the right one too.

        If there is no AND, it will return the left factor as is.
        """
        factor = self.parse_factor()
        while self.is_operator("AND"):
            op = self.current_token.value.upper()
            self.advance()
            right = self.parse_factor()
            factor = {"type": "conjunction", "op": op, "left": factor, "right": right}
        return factor

    def parse_factor(self) -> dict:
        """Parse a factor, which are comparison expressions possibly negated by NOT.

        If the factor is negated by NOT, it will parse the operand and return
        it wrapped in a unary operation node.

        If the factor is not negated, it will move forward an parse the eventual comparison.
        """
        if self.is_operator("NOT"):
            self.advance()
            operand = self.parse_factor()
            return {"type": "unary_op", "op": "NOT", "operand": operand}
        else:
            return self.parse_comparison()

    def parse_comparison(self) -> dict:
        """Parse a comparison, which is are mathematical expression compared via a comparison operator.

        Parses the left side of the comparison and then if there is a comparison operator
        it will parse the right side too.

        If there is no comparison operator, it will return the left side as is.
        """
        left = self.parse_additive_expr()
        if self.is_operator("=", "<", ">", "<=", ">=", "<>", "!="):
            op = self.current_token.value
            self.advance()
            right = self.parse_additive_expr()
            return {"type": "comparison", "op": op, "left": left, "right": right}
        else:
            # No comparison operator, return the left expression
            return left

    def parse_additive_expr(self) -> dict:
        """Parse addition and subtraction expressions as they have math precedence.

        Parses left multiplications and divisions first and then if there is an
        addition or subtraction it will parse the right side too.

        If there is no addition or subtraction, it will return the left side as is.
        """
        expr = self.parse_multiplicative_expr()
        while self.is_operator("+", "-"):
            op = self.current_token.value
            self.advance()
            right = self.parse_multiplicative_expr()
            expr = {"type": "binary_op", "op": op, "left": expr, "right": right}
        return expr

    def parse_multiplicative_expr(self) -> dict:
        """Parse multiplication and division expressions.

        Parses unary operators that might be applied to the left primary value
        and then if there is a multiplication or division it will parse the right side too.

        If there is no multiplication or division, it will return the left side as is.
        """
        expr = self.parse_unary_expr()
        while self.is_operator("*", "/"):
            op = self.current_token.value
            self.advance()
            right = self.parse_unary_expr()
            expr = {"type": "binary_op", "op": op, "left": expr, "right": right}
        return expr

    def parse_unary_expr(self) -> dict:
        """Parse unary mathematical expressions. Like -X

        If there is no unary operator, it will just move forward and parse the primary value.
        """
        if self.is_operator("-"):
            op = self.current_token.value
            self.advance()
            operand = self.parse_unary_expr()
            return {"type": "unary_op", "op": op, "operand": operand}
        else:
            return self.parse_primary()

    def parse_primary(self) -> dict:
        """Parse primary expressions: function calls, atoms, or parenthesis expressions.

        Primary expressions are the lowest level of the expression grammar and can be
        identifiers, literals, or function calls. Those take precedence over anything else
        and thus are parsed first (you have to first parse SUM(x) before you can multiply it by 2).

        If the primary expression is wrapped in parenthesis, it will parse the expression inside
        the parenthesis and return it.
        """
        if self.is_punctuation("("):
            self.advance()
            expr = self.parse_expression()
            if not self.is_punctuation(")"):
                raise SQLExpressionError("Expected ')'")
            self.advance()
            return expr
        else:
            return self.parse_atom()

    def parse_atom(self) -> dict:
        """Parse an identifier, literal, or function call.

        If there is a function call, it will parse the function name and arguments.

        In case of literals it also tries to cast them to Python values.
        """
        token = self.current_token
        if isinstance(token, IdentifierToken):
            identifier = token.value
            self.advance()
            if self.is_punctuation("("):
                return self.parse_function_call(identifier)
            else:
                return {"type": "identifier", "value": identifier}
        elif isinstance(token, LiteralToken):
            value = token.value
            self.advance()
            return {"type": "literal", "value": self.cast_literal(value)}
        else:
            raise SQLExpressionError(f"Unexpected token: {token}")

    def parse_function_call(self, function_name: str) -> dict:
        """Parse a function call.

        If there is a function call, it will parse the arguments.
        The arguments of the call are parsed as expressions too. So they restart the parsing.

        If there is no function call it will raise an error as we expect the caller
        to have already determined that it is a function call and have provided
        that's the function.
        """
        self.advance()  # Consume '('
        args = []
        if not self.is_punctuation(")"):
            while True:
                arg = self.parse_expression()
                args.append(arg)
                if self.is_punctuation(","):
                    self.advance()
                else:
                    break
        if not self.is_punctuation(")"):
            raise SQLExpressionError("Expected ')'")
        self.advance()  # Consume ')'
        return {"type": "function_call", "name": function_name, "args": args}

    def is_operator(self, *ops: str) -> bool:
        """Check if the current token is an OperatorToken with a value in ops.

        This is used to check is one of ``+ - * / = < > <= >= <> !=`` is the current token.
        """
        return isinstance(
            self.current_token, OperatorToken
        ) and self.current_token.value.upper() in [op.upper() for op in ops]

    def is_punctuation(self, *chars: str) -> bool:
        """Check if the current token is a PunctuationToken with a value in chars.

        This is used to check if one of ``( ) ,`` is the current token.
        """
        return (
            isinstance(self.current_token, PunctuationToken)
            and self.current_token.value in chars
        )

    def cast_literal(self, value: str) -> str | float | int:
        """Cast a literal in a SQL expression to a Python value.

        As the lexer returns literals as strings, we need to  try to detect
        if the string represents an integer, a float, or a string and cast it
        to the appropriate Python type.
        """
        if value[0] == value[-1] in ("'", '"'):
            return value[1:-1]
        else:
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    return value


class SQLExpressionError(Exception):
    """Exception raised for errors in SQL expression parsing."""

    pass
