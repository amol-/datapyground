import pyarrow.compute as pc

from datapyground.compute import FunctionCallExpression
from datapyground.dataframe import Dataframe, col

df = Dataframe.open("shops.csv") \
  .filter(FunctionCallExpression(pc.equal, col("City"), "Rome")) \
  .filter(FunctionCallExpression(pc.greater_equal, FunctionCallExpression(pc.find_substring, col("Shop Name"), "Shop 4"), 0)) \
  .collect()

print(df)
