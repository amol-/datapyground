"""Expressions executed by compute engine nodes.

The Compute Engine will sometimes need to filter data
or emit new data. This will be performed by nodes that
need to know how the data must be filtered or emitted.

Filters will need a ``predicate``, so an expression that
returns ``true`` or ``false`` for each row that has to be
filtered.

Projections will need an expression that computes the rows
for the projection, for example ``A + B``.

More node types might need different type of expressions.
This module implements the most common ones.
"""
import pyarrow as pa

from .. import utils
from .base import Expression


def apply_expression_if_needed(batch: pa.RecordBatch, o: Expression|pa.Array) -> pa.Array:
    """Invoke Apply on expressions when needed
    
    If the provided object is an Expression,
    it will be applied to the target batch.

    Otherwise it will treat it as if it's
    already the result of an expression
    or a literal value.

    This allows us to apply all arguments
    we receive without having to care if
    they are the data we need or if they
    are the expression resulting in that data.
    """
    if isinstance(o, Expression):
        o = o.apply(batch)
    return o


class FunctionCallExpression(Expression):
    """Call a compute function on its arguments.
    
    Given a compute function, and a set of arguments
    (other expressions, literals or data), execute
    the function on the provided arguments and return
    the resulting data.

    For example to sum two columns this would be used as::

        FunctionCallExpression(pyarrow.compute.sum, [ColumnRef("A"), ColumnRef("B")])
    
    """
    def __init__(self, func: callable, *args: Expression) -> None:
        """
        :param func: The function accepting the arguments.
        :param *args: The arguments for the function.
        """
        self.func = func
        self.args = args

    def __str__(self) -> str:
        func_qualname = utils.inspect.get_qualname(self.func)
        return f"{func_qualname}({','.join(map(str, self.args))})"

    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        """Invoke the function resolving all argumnets on the recordbatch.
        
        When the function arguments are expressions themselves,
        this will apply the expressions on the provided recordbatch
        and the resulting data will be used as the arguments for the
        function.
        """
        args = tuple(
            apply_expression_if_needed(batch, arg) for arg in self.args
        )
        return self.func(*args)