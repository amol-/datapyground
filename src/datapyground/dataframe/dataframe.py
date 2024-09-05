import pyarrow as pa

from ..compute import PyArrowTableDataSource, CSVDataSource, FilterNode, QueryPlanNode


class Dataframe:
  def __init__(self, node_or_table):
    if isinstance(node_or_table, pa.Table):
      node_or_table = PyArrowTableDataSource(node_or_table)
    
    if not isinstance(node_or_table, QueryPlanNode):
      raise ValueError("Invalid input, expected a QueryPlanNode or a PyArrow Table")

    self.node = node_or_table

  @classmethod
  def open_csv(cls, filename):
    return cls(CSVDataSource(filename))

  def filter(self, expression):
    return Dataframe(FilterNode(expression, self.node))

  def collect(self):
    return pa.Table.from_batches(self.node.batches())