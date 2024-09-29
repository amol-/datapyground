"""A simple regular expression-based tokenizer for SQL queries.

Given a text string containing an SQL query, this module provides a simple
tokenizer that converts the input text into a sequence of tokens.

Each token will be an object of a different class depending on the type of token,
this allows for the parser to dispach parsing code based on the type of token
and build the abstract syntax tree (AST) of the query.

This approach has some notable limitations, primarily in the context
of tokenizing literals:

- Nested quotes in string literals are not correctly supported.
- The tokenizer does not support comments.
- The tokenizer does not support escape characters in string literals.
- The tokenizer does not support escape sequences in string literals.

For a more robust parser, you would typically use a dedicated library like
SQLGlot or Calcite. But for the purposes of DataPyground, this simple tokenizer
is good enough and it serves the purpose of showcasing how SQL queries can be
parsed and executed.

The main class in this module is the :class:`Tokenizer` class, which is responsible
for the tokenization process itself.
"""

import re


def GENERATE_TOKEN_SPECIFICATION() -> list[tuple[str, str]]:
    """Provides the token specification for the tokenizer.

    Each entry in the specification is a tuple of (token_name, regex_pattern).
    The order of the entries is important as it defines the priority of the tokens.
    For example, the KEYWORD token should be before the IDENTIFIER token
    because if a keyword is matched, it should not be matched as an identifier.
    """
    return [
        (
            "KEYWORD",
            r"SELECT|INSERT|UPDATE|FROM|WHERE|GROUP BY|ORDER BY|ASC|DESC|LIMIT|OFFSET|AS",
        ),
        ("TEXT_OPERATOR", r"\b(AND|OR|NOT)\b"),
        ("OPERATOR", r"<>|<=|>=|!=|==|=|<|>|\+|-|\*|/"),
        (
            "IDENTIFIER",
            r"[A-Za-z_][A-Za-z0-9_\.]*",
        ),  # like tablename, column_name, tablename.column_name
        ("LITERAL", r"\'[^\']*\'|\"[^\"]*\"|\d+(\.\d+)?"),
        ("PUNCTUATION", r",|\(|\)|;"),
        ("SKIP", r"\s+"),  # Skip spaces and tabs
        ("MISMATCH", r"."),  # Any other character
    ]


def GENERATE_TOKENIZATION_REGEX() -> re.Pattern:
    """Combine the token specification into a regex pattern for tokenization.

    This will generate the regular expression that the tokenizer will use to
    match tokens in the input text.

    The regular expression is generated by combining the regex patterns of the
    token specification into a single regex pattern that will match any of the
    tokens.

    When a token is matched, the group name of the match will be the name of the
    token type, which will be used to dispatch the parsing code based on the type

    For example ``"SELECT"`` will be matched by the ``KEYWORD`` token of the specification
    and as the name of the token in the specification is also the group name of the match
    in the constructed regular expression, the match group will be ``'KEYWORD'``.
    """
    return re.compile(
        "|".join("(?P<%s>%s)" % pair for pair in GENERATE_TOKEN_SPECIFICATION()),
        re.IGNORECASE,
    )


class Token:
    """Base class for all token types.

    Every token will have a value attribute that represents the
    text value of the token as it appears in the input query.
    """

    def __init__(self, value: str) -> None:
        """
        :param value: The text value of the token.
        """
        self.value = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.value == other.value


class Tokenizer:
    """A simple regular expression-based tokenizer for SQL queries.

    Given a text string containing an SQL query, this class uses
    the regular expression generated by :func:`GENERATE_TOKENIZATION_REGEX`
    to process the input text and convert it into a sequence of tokens.

    The tokens can subsequently be used by the parser to build the abstract
    syntax tree (AST) of the query.

    The tokenizer takes for granted that the input text is a valid SQL SELECT query.
    If the input text is not a valid SQL query, the tokenizer may raise exceptions
    or have unpredictable behavior. This is a limitation of the simple regex-based
    approach used in this tokenizer. Also given that the parser doesn't support
    queries of any type other than SELECT queries, the tokenizer is not designed
    to handle other types of queries.

    The tokenizer works by matching the regular expression against the text,
    once a match is found, an object of the corresponding token class is created
    and added to the list of tokens.
    The tokenizer then advances to the end of the matched token and continues
    matching tokens until the end of the text is reached.

    For example::

              SELECT id FROM table WHERE age >= 18
              ^      ^  ^    ^     ^     ^   ^  ^
        pos = 0      7  10   15    20    25  28 31

    would be tokenized into a sequence of tokens like::

        SelectToken, IdentifierToken, FromToken, IdentifierToken, WhereToken, IdentifierToken, OperatorToken, LiteralToken

    .. note::

        The tokenizer is not thread-safe
    """

    TOKENIZATION_REGEX = GENERATE_TOKENIZATION_REGEX()

    def __init__(self, text: str) -> None:
        """
        :params text: The input text containing the SQL query to tokenize.
        """
        self.pos = 0
        self.text = text

        #: Mapping of the keyword token values to the token class
        self.keyword_token_classes = {
            "SELECT": SelectToken,
            "INSERT": InsertToken,
            "UPDATE": UpdateToken,
            "FROM": FromToken,
            "WHERE": WhereToken,
            "GROUP BY": GroupByToken,
            "ORDER BY": OrderByToken,
            "LIMIT": LimitToken,
            "OFFSET": OffsetToken,
            "ASC": SortingOrderToken,
            "DESC": SortingOrderToken,
            "AS": AliasToken,
        }

    def tokenize(self) -> list[Token]:
        """Tokenize the input text into a sequence of tokens."""
        tokens: list[Token] = []

        self.advance_to(0)  # Reset the tokenizer to start from the beginning

        mo = self.get_next_token()
        while mo is not None:
            kind = mo.lastgroup
            if kind is None:
                raise SQLTokenizeException(
                    f"Unexpected character {mo.group()!r} at position {self.pos}"
                )
            value = mo.group(kind)
            if kind == "SKIP":
                pass
            elif kind == "MISMATCH":
                raise SQLTokenizeException(
                    f"Unexpected character {value!r} at position {self.pos}"
                )
            else:
                token: Token
                if kind == "KEYWORD":
                    cls = self.keyword_token_classes.get(value.upper(), KeywordToken)
                    token = cls(value)
                elif kind == "IDENTIFIER":
                    token = IdentifierToken(value)
                elif kind in ("OPERATOR", "TEXT_OPERATOR"):
                    token = OperatorToken(value)
                elif kind == "LITERAL":
                    token = LiteralToken(value)
                elif kind == "PUNCTUATION":
                    token = PunctuationToken(value)
                else:
                    raise SQLTokenizeException(f"Unknown token type: {kind}")
                tokens.append(token)
            self.advance_to(
                mo.end()
            )  # move the tokenizer to the end of the matched token
            mo = self.get_next_token()  # repeat for the next token

        return tokens

    def get_next_token(self) -> re.Match[str] | None:
        """Match the next token in the input text from the current tokenizer position."""
        return self.TOKENIZATION_REGEX.match(self.text, self.pos)

    def advance_to(self, pos: int) -> None:
        """Advance the tokenizer to a new position in the input text.

        :param pos: The new position to which to advance the tokenizer.

        Subsequent calls to :meth:`get_next_token` will start from the new position
        and only match tokens that come after the new position.

        The tokenizer will automatically advance during the tokenization process,
        there is no need to invoke this manually.
        """
        self.pos = pos


class KeywordToken(Token):
    """Base class for all SQL Keywords.

    Keywords are always rappresented in uppercase
    as a convention to distinguish them from other tokens.
    """

    def __init__(self, value: str) -> None:
        """
        :param value: The text value of the keyword token.
        """
        super().__init__(value.upper())


class SelectToken(KeywordToken):
    """Token representing the SELECT keyword."""

    pass


class InsertToken(KeywordToken):
    """Token representing the INSERT keyword."""

    pass


class UpdateToken(KeywordToken):
    """Token representing the UPDATE keyword."""

    pass


class FromToken(KeywordToken):
    """Token representing the FROM keyword."""

    pass


class WhereToken(KeywordToken):
    """Token representing the WHERE keyword."""

    pass


class GroupByToken(KeywordToken):
    """Token representing the GROUP BY keyword."""

    pass


class OrderByToken(KeywordToken):
    """Token representing the ORDER BY keyword."""

    pass


class LimitToken(KeywordToken):
    """Token representing the LIMIT keyword."""

    pass


class OffsetToken(KeywordToken):
    """Token representing the OFFSET keyword."""

    pass


class SortingOrderToken(KeywordToken):
    """Token representing the ASC and DESC keywords."""

    pass


class AliasToken(KeywordToken):
    """Token representing the AS keyword."""

    pass


class IdentifierToken(Token):
    """Token representing an identifier (table name, column name, etc)."""

    pass


class OperatorToken(Token):
    """Token representing an operator (comparison, arithmetic, etc).

    Operators are always rappresented in uppercase if they are text operators.
    """

    def __init__(self, value: str) -> None:
        """
        :param value: The text value of the operator token.
        """
        super().__init__(value.upper())


class LiteralToken(Token):
    """Token representing a literal value (string, number, etc)."""

    pass


class PunctuationToken(Token):
    """Token representing a punctuation character (comma, parenthesis, etc)."""

    pass


class EOFToken(Token):
    """Special Token representing the end of the input text."""

    def __init__(self) -> None:
        """Value is hardcoded to EOF"""
        super().__init__("EOF")


class SQLTokenizeException(ValueError):
    """Exception raised when an error occurs during tokenization."""

    pass
