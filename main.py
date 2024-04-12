# ValueError: Some of types cannot be determined after inferring

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


spark = SparkSession.builder.getOrCreate()

df = pd.DataFrame({
    'name': ['Alice', 'Bobby', 'Carl', 'Dan'],
    'experience': [11, 14, 16, 18],
    'salary': [None, None, None, None],
})

schema = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("experience", IntegerType(), nullable=True),
        StructField("salary", DoubleType(), nullable=True),

    ]
)

new_df = spark.createDataFrame(df, schema=schema)

# DataFrame[name: string, experience: int, salary: double]
print(new_df)