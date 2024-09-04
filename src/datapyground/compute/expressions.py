import pyarrow as pa

from .base import Expression


def apply_expression_if_needed(batch, o):
    """Invoke Apply on expressions when needed
    
    If the provided object is an Expression,
    it will be applied to the target batch.

    Otherwise it will treat it as if it's
    already the result of an expression.

    This allows us to apply all arguments
    we receive without having to care if
    they are the data we need or if they
    are the expression resulting in that data.
    """
    if isinstance(o, Expression):
        o = o.apply(batch)
    return o


class FunctionCallExpression(Expression):
    def __init__(self, func, *args) -> None:
        self.func = func
        self.args = args

    def __str__(self) -> str:
        return f"{self.func}({','.join(self.args)})"

    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        args = tuple(
            apply_expression_if_needed(batch, arg) for arg in self.args
        )
        return self.func(*args)