import pyarrow.compute as pc

from datapyground.compute import CSVDataSource, FilterNode, FunctionCallExpression, col

query = FilterNode(
    FunctionCallExpression(pc.equal, col("City"), "Rome"),
    CSVDataSource("data/shops.csv"),
)
for batch in query.batches():
    print("---")
    print(batch)
