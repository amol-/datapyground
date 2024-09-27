import pytest

from datapyground.sql.tokenize import (
    EOFToken,
    FromToken,
    IdentifierToken,
    InsertToken,
    LiteralToken,
    OperatorToken,
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
