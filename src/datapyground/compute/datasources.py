"""Query Plan nodes that load data

The datasource nodes are expected to fetch the data from some source,
convert it into the format accepted by the compute engine and forward it
to the next node in the plan.

They are used to do things like loading
data from CSV files or equivalent operations
"""

import pyarrow as pa
import pyarrow.csv

from .base import QueryPlanNode


class CSVDataSource(QueryPlanNode):
    """Load data from a CSV file.

    Given a local CSV file path, load the content,
    convert it into Arrow format, and emit it
    for the next nodes of the query plan to consume.
    """

    def __init__(self, filename: str, block_size: int | None = None) -> None:
        """
        :param filename: The path of the local CSV file.
        :param block_size: How big to make batches of data,
                           Influences how many batches will be produced
        """
        self.filename = filename
        self.block_size = block_size

    def __str__(self) -> str:
        return f"CSVDataSource({self.filename}, block_size={self.block_size})"

    def batches(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Open CSV file and emit the batches."""
        with pa.csv.open_csv(
            self.filename, read_options=pa.csv.ReadOptions(block_size=self.block_size)
        ) as reader:
            for batch in reader:
                yield batch


class PyArrowTableDataSource(QueryPlanNode):
    """Load data from an in-memory pyarrow.Table or pyarrow.RecordBatch.

    Given a :class:`pyarrow.Table` or `pyarrow.RecordBatch` object,
    allow to use its data in a query plan.
    """

    def __init__(self, table: pa.Table | pa.RecordBatch) -> None:
        """
        :param table: The table or recordbatch with the data to read.
        """
        self.table = table
        self.is_recordbatch = isinstance(table, pa.RecordBatch)

    def __str__(self) -> str:
        return f"PyArrowTableDataSource(columns={self.table.column_names}, rows={self.table.num_rows})"

    def batches(self) -> QueryPlanNode.RecordBatchesGenerator:
        """Emit the data contained in the Table for consumption by other node."""
        if self.is_recordbatch:
            yield self.table
        else:
            yield from self.table.to_batches()
