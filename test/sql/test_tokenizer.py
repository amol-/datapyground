import pytest

from datapyground.sql.tokenize import (
    EOFToken,
    FromToken,
    GroupByToken,
    IdentifierToken,
    InsertToken,
    LimitToken,
    LiteralToken,
    OffsetToken,
    OperatorToken,
    OrderByToken,
    PunctuationToken,
    SelectToken,
    SQLTokenizeException,
    Tokenizer,
    UpdateToken,
    WhereToken,
)


def test_tokenizer_select_query():
    query = "SELECT id FROM table WHERE age >= 18"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        FromToken("FROM"),
        IdentifierToken("table"),
        WhereToken("WHERE"),
        IdentifierToken("age"),
        OperatorToken(">="),
        LiteralToken("18"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_insert_query():
    query = "INSERT INTO table (id, name) VALUES (1, 'John')"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        InsertToken("INSERT"),
        IdentifierToken("INTO"),
        IdentifierToken("table"),
        PunctuationToken("("),
        IdentifierToken("id"),
        PunctuationToken(","),
        IdentifierToken("name"),
        PunctuationToken(")"),
        IdentifierToken("VALUES"),
        PunctuationToken("("),
        LiteralToken("1"),
        PunctuationToken(","),
        LiteralToken("'John'"),
        PunctuationToken(")"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_update_query():
    query = "UPDATE table SET name = 'John' WHERE id = 1"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        UpdateToken("UPDATE"),
        IdentifierToken("table"),
        IdentifierToken("SET"),
        IdentifierToken("name"),
        OperatorToken("="),
        LiteralToken("'John'"),
        WhereToken("WHERE"),
        IdentifierToken("id"),
        OperatorToken("="),
        LiteralToken("1"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_unexpected_character():
    query = "SELECT id FROM table WHERE age >= 18 @"
    tokenizer = Tokenizer(query)
    with pytest.raises(SQLTokenizeException) as excinfo:
        tokenizer.tokenize()
    assert "Unexpected character '@'" in str(excinfo.value)


def test_tokenizer_empty_query():
    query = ""
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [EOFToken()]

    assert tokens == expected_tokens


def test_tokenizer_whitespace_query():
    query = "   "
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [EOFToken()]

    assert tokens == expected_tokens


def test_tokenizer_complex_query():
    query = "SELECT id, name FROM table WHERE age >= 18 AND name = 'John'"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        PunctuationToken(","),
        IdentifierToken("name"),
        FromToken("FROM"),
        IdentifierToken("table"),
        WhereToken("WHERE"),
        IdentifierToken("age"),
        OperatorToken(">="),
        LiteralToken("18"),
        IdentifierToken("AND"),
        IdentifierToken("name"),
        OperatorToken("="),
        LiteralToken("'John'"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_limit_query():
    query = "SELECT id FROM table LIMIT 10"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        FromToken("FROM"),
        IdentifierToken("table"),
        LimitToken("LIMIT"),
        LiteralToken("10"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_offset_query():
    query = "SELECT id FROM table LIMIT 10 OFFSET 5"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        FromToken("FROM"),
        IdentifierToken("table"),
        LimitToken("LIMIT"),
        LiteralToken("10"),
        OffsetToken("OFFSET"),
        LiteralToken("5"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_order_by_query():
    query = "SELECT id FROM table ORDER BY name ASC"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        FromToken("FROM"),
        IdentifierToken("table"),
        OrderByToken("ORDER BY"),
        IdentifierToken("name"),
        IdentifierToken("ASC"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_tokenizer_group_by_query():
    query = "SELECT id, COUNT(*) FROM table GROUP BY id"
    tokenizer = Tokenizer(query)
    tokens = tokenizer.tokenize()

    expected_tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        PunctuationToken(","),
        IdentifierToken("COUNT"),
        PunctuationToken("("),
        OperatorToken("*"),
        PunctuationToken(")"),
        FromToken("FROM"),
        IdentifierToken("table"),
        GroupByToken("GROUP BY"),
        IdentifierToken("id"),
        EOFToken(),
    ]

    assert tokens == expected_tokens


def test_token_str_repr():
    tokens = [
        SelectToken("SELECT"),
        IdentifierToken("id"),
        FromToken("FROM"),
        IdentifierToken("table"),
        WhereToken("WHERE"),
        IdentifierToken("age"),
        OperatorToken(">="),
        LiteralToken("18"),
        EOFToken(),
    ]

    for token in tokens:
        assert repr(token) == f"{token.__class__.__name__}({token.value!r})"


def test_token_equality():
    token1 = SelectToken("SELECT")
    token2 = SelectToken("SELECT")
    token3 = FromToken("FROM")
    non_token = "SOME TEXT"

    assert token1 == token2
    assert token1 != token3
    assert token1 != non_token
