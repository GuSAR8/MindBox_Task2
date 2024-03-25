from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("ProductAnalysis") \
    .getOrCreate()

products_data = [
    ("product1",),
    ("product2",),
    ("product3",)
    ]
products_df = spark.createDataFrame(products_data, ["product"])

categories_data = [
    ("product1", "category1"),
    ("product1", "category2"),
    ("product2", "category1")
    ]
categories_df = spark.createDataFrame(
    categories_data,
    ["product", "category"]
    )

product_category = products_df.join(
    categories_df,
    "product",
    "left_outer"
    )

products_without_categories = products_df.join(
    categories_df,
    "product",
    "left_anti"
    )

print("Все пары «Имя продукта – Имя категории»:")
product_category.show()

print("Имена всех продуктов, у которых нет категорий:")
products_without_categories.show()

spark.stop()
