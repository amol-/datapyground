"""The Dataframe object itself."""
from typing import Self

import pyarrow as pa

from ..compute import CSVDataSource, FilterNode, PyArrowTableDataSource
from ..compute.base import QueryPlanNode
from ..compute.expressions import Expression


class Dataframe:
  """Data structure that handles data in rows and columns.

  The Dataframe object allows to represent in-memory data
  and perform transformations over it.

  The datapyground dataframe object is lazy, which means that
  any transformation or analysis will be applied only when the
  ``.collect()`` method will be invoked and no data is kept
  in memory until that moment (unless it already was).
  """
  def __init__(self, node_or_table: QueryPlanNode|pa.Table) -> None:
    """
    :param node_or_table: A compute engine node expected to emit 
                          the data for the dataframe or a `pyarrow.Table`.
    """
    if isinstance(node_or_table, pa.Table):
      node_or_table = PyArrowTableDataSource(node_or_table)
    
    if not isinstance(node_or_table, QueryPlanNode):
      raise ValueError("Invalid input, expected a QueryPlanNode or a PyArrow Table")

    self.node = node_or_table

  @classmethod
  def open_csv(cls, filename: str) -> Self:
    """Open a CSV file and create a Dataframe out of its data.
    
    :param filename: The path to a local CSV file.
    """
    return cls(CSVDataSource(filename))

  def filter(self, expression: Expression) -> Self:
    """Apply a filter to the data and return a new Dataframe.
    
    The returned dataframe will only contain the data that
    matches the filter predicate.

    :param expression: The expression representing the predicate.
                       for example `A > B`.
    """
    return self.__class__(FilterNode(expression, self.node))

  def collect(self) -> Self:
    """Collect all data of the dataframe in memory.
    
    Returns a new Dataframe that has all data from the
    previous dataframe eagerly loaded in memory.
    """
    return self.__class__(pa.Table.from_batches(self.node.batches()))

  def to_arrow(self) -> pa.Table:
    """Collect all the data and return a pyarrow.Table"""
    return pa.Table.from_batches(self.node.batches())