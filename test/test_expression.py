import pytest
import pyarrow as pa
import pyarrow.compute as pc
from datapyground.compute.expressions import FunctionCallExpression
from datapyground.compute.base import ColumnRef

@pytest.fixture
def sample_batch():
    return pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5]), pa.array(['a', 'b', 'c', 'd', 'e'])],
        names=['numbers', 'letters']
    )

def test_function_call_expression_init():
    expr = FunctionCallExpression(pc.add, ColumnRef('numbers'), 1)
    assert expr.func == pc.add
    assert len(expr.args) == 2
    assert isinstance(expr.args[0], ColumnRef)
    assert expr.args[1] == 1

def test_function_call_expression_str():
    expr = FunctionCallExpression(pc.add, ColumnRef('numbers'), 1)
    assert str(expr) == "pyarrow.compute.add(ColumnRef(numbers),1)"

def test_function_call_expression_apply_simple(sample_batch):
    expr = FunctionCallExpression(pc.add, ColumnRef('numbers'), 1)
    result = expr.apply(sample_batch)
    expected = pa.array([2, 3, 4, 5, 6])
    assert result.equals(expected)

def test_function_call_expression_apply_nested(sample_batch):
    inner_expr = FunctionCallExpression(pc.multiply, ColumnRef('numbers'), 2)
    outer_expr = FunctionCallExpression(pc.add, inner_expr, 1)
    result = outer_expr.apply(sample_batch)
    expected = pa.array([3, 5, 7, 9, 11])
    assert result.equals(expected)

def test_function_call_expression_apply_string_ops(sample_batch):
    expr = FunctionCallExpression(pc.utf8_upper, ColumnRef('letters'))
    result = expr.apply(sample_batch)
    expected = pa.array(['A', 'B', 'C', 'D', 'E'])
    assert result.equals(expected)

def test_function_call_expression_apply_comparison(sample_batch):
    expr = FunctionCallExpression(pc.greater, ColumnRef('numbers'), 3)
    result = expr.apply(sample_batch)
    expected = pa.array([False, False, False, True, True])
    assert result.equals(expected)

def test_function_call_expression_apply_multiple_args(sample_batch):
    expr = FunctionCallExpression(pc.if_else, 
                                  FunctionCallExpression(pc.greater, ColumnRef('numbers'), 3),
                                  ColumnRef('letters'),
                                  'x')
    result = expr.apply(sample_batch)
    expected = pa.array(['x', 'x', 'x', 'd', 'e'])
    assert result.equals(expected)

def test_function_call_expression_apply_null_handling(sample_batch):
    numbers_with_null = pa.array([1, None, 3, 4, 5])
    batch_with_null = pa.RecordBatch.from_arrays([numbers_with_null, sample_batch['letters']], names=['numbers', 'letters'])
    expr = FunctionCallExpression(pc.add, ColumnRef('numbers'), 1)
    result = expr.apply(batch_with_null)
    expected = pa.array([2, None, 4, 5, 6])
    assert result.equals(expected)

def test_function_call_expression_apply_invalid_column():
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], names=['numbers'])
    expr = FunctionCallExpression(pc.add, ColumnRef('non_existent'), 1)
    with pytest.raises(KeyError):
        expr.apply(batch)

def test_function_call_expression_apply_type_mismatch():
    batch = pa.RecordBatch.from_arrays([pa.array(['a', 'b', 'c'])], names=['letters'])
    expr = FunctionCallExpression(pc.add, ColumnRef('letters'), 1)
    with pytest.raises(pa.ArrowNotImplementedError):
        expr.apply(batch)
