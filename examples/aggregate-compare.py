import sys
import time

import pandas
import psutil

from datapyground.compute import AggregateNode, CSVDataSource, SumAggregation

try:
    aggregation_type = sys.argv[1]
except IndexError:
    aggregation_type = None

if aggregation_type == "single":
    # On development machine did lead to
    #   TIME: 1.1 MEMORY: 167
    q = AggregateNode(
        ["year"],
        {"total_geo_count": SumAggregation("geo_count")},
        child=CSVDataSource("data/geounits.csv", block_size=1024 * 1024),
    )
elif aggregation_type == "multi":
    # On development machine did lead to
    #   TIME: 46.7 MEMORY: 169
    q = AggregateNode(
        ["year", "Area"],
        {"total_geo_count": SumAggregation("geo_count")},
        child=CSVDataSource("data/geounits.csv", block_size=1024 * 1024),
    )
elif aggregation_type == "pandas":
    # On development machine did lead to
    #   TIME: 2.1 MEMORY: 116
    class FakeQuery:
        def batches(self):
            df = pandas.read_csv("data/geounits.csv")
            yield (
                df.groupby("year")
                .agg({"geo_count": "sum"})
                .rename(columns={"geo_count": "total_geo_count"})
            )

    q = FakeQuery()
else:
    print("Aggregation must be single, multi or pandas")
    sys.exit(1)

proc = psutil.Process()
start = time.time()
for b in q.batches():
    continue
end = time.time()

print(
    "TIME:",
    round(end - start, 1),
    "MEMORY:",
    proc.memory_full_info().rss // (1024 * 1024),
)
