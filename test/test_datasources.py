import os
import tempfile

import pyarrow as pa
import pyarrow.csv as csv
import pytest

from datapyground.compute.datasources import CSVDataSource, PyArrowTableDataSource

# Mock data for testing
MOCK_PYARROW_TABLE = pa.table({"col1": [1, 4, 7], "col2": [2, 5, 8], "col3": [3, 6, 9]})

MOCK_CSV_FILE = tempfile.NamedTemporaryFile(delete=False, mode="w+")


def setup_module():
    csv.write_csv(MOCK_PYARROW_TABLE, MOCK_CSV_FILE.name)
    MOCK_CSV_FILE.close()


def teardown_module():
    os.unlink(MOCK_CSV_FILE.name)


@pytest.mark.parametrize(
    "data_source_class, init_args, expected_str",
    [
        (
            CSVDataSource,
            (MOCK_CSV_FILE.name, None),
            f"CSVDataSource({MOCK_CSV_FILE.name}, block_size=None)",
        ),
        (
            PyArrowTableDataSource,
            (MOCK_PYARROW_TABLE,),
            "PyArrowTableDataSource(columns=['col1', 'col2', 'col3'], rows=3)",
        ),
        (
            PyArrowTableDataSource,
            (MOCK_PYARROW_TABLE.to_batches()[0],),
            "PyArrowTableDataSource(columns=['col1', 'col2', 'col3'], rows=3)",
        ),
    ],
)
def test_init_and_str(data_source_class, init_args, expected_str):
    data_source = data_source_class(*init_args)
    assert str(data_source) == expected_str


@pytest.mark.parametrize(
    "data_source_class, init_args, expected_batches",
    [
        (CSVDataSource, (MOCK_CSV_FILE.name, None), MOCK_PYARROW_TABLE.to_batches()),
        (
            PyArrowTableDataSource,
            (MOCK_PYARROW_TABLE,),
            MOCK_PYARROW_TABLE.to_batches(),
        ),
        (
            PyArrowTableDataSource,
            (MOCK_PYARROW_TABLE.to_batches()[0],),
            MOCK_PYARROW_TABLE.to_batches(),
        ),
    ],
)
def test_batches(data_source_class, init_args, expected_batches):
    data_source = data_source_class(*init_args)
    batches = list(data_source.batches())
    assert len(batches) == len(expected_batches)
    for batch, expected_batch in zip(batches, expected_batches):
        assert batch.equals(expected_batch)
