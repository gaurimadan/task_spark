from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    trim,
    to_date,
    year
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# ============================================================
# 1. Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gutenberg Author Influence Network")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Dataset
# ============================================================

DATASET_PATH = "/home/gauri/Downloads/D184MB"


# ============================================================
# 3. Load complete books
# ============================================================

rdd = spark.sparkContext.wholeTextFiles(
    DATASET_PATH
)

books_df = (
    rdd
    .map(
        lambda x: (
            x[0].split("/")[-1],
            x[1]
        )
    )
    .toDF(["file_name", "text"])
)

print(
    "Total books:",
    books_df.count()
)


# ============================================================
# 4. Extract author and release date
# ============================================================

metadata_df = (
    books_df

    .withColumn(
        "author",
        trim(
            regexp_extract(
                col("text"),
                r"(?im)^Author:\s*(.+)$",
                1
            )
        )
    )

    .withColumn(
        "release_date",
        trim(
            regexp_extract(
                col("text"),
                r"(?im)^Release Date:\s*(.+)$",
                1
            )
        )
    )
)


# ============================================================
# 5. Extract year
# ============================================================

metadata_df = metadata_df.withColumn(
    "release_year",
    regexp_extract(
        col("release_date"),
        r"(\d{4})",
        1
    ).cast("int")
)


print("\n========== AUTHOR METADATA ==========")

metadata_df.select(
    "file_name",
    "author",
    "release_date",
    "release_year"
).show(
    20,
    truncate=False
)


# ============================================================
# 6. Remove missing/invalid records
# ============================================================

valid_df = (
    metadata_df
    .filter(
        (trim(col("author")) != "")
        &
        col("release_year").isNotNull()
    )
    .select(
        "file_name",
        "author",
        "release_year"
    )
)

print(
    "\nValid books:",
    valid_df.count()
)


# ============================================================
# 7. Create author-year records
# ============================================================

author_books = (
    valid_df
    .select(
        "author",
        "release_year"
    )
    .dropDuplicates()
)


# ============================================================
# 8. Create author pairs within 5 years
# ============================================================

a = author_books.alias("a")

b = author_books.alias("b")

edges = (
    a.crossJoin(b)
    .filter(
        col("a.author") != col("b.author")
    )
    .filter(
        (
            col("b.release_year")
            >
            col("a.release_year")
        )
        &
        (
            col("b.release_year")
            <=
            col("a.release_year") + 5
        )
    )
    .select(
        col("a.author").alias("author1"),
        col("b.author").alias("author2")
    )
    .dropDuplicates()
)


# ============================================================
# 9. Display sample edges
# ============================================================

print("\n========== SAMPLE INFLUENCE EDGES ==========")

edges.show(
    20,
    truncate=False
)


# ============================================================
# 10. Out-degree
# ============================================================

out_degree = (
    edges
    .groupBy("author1")
    .count()
    .withColumnRenamed(
        "author1",
        "author"
    )
    .withColumnRenamed(
        "count",
        "out_degree"
    )
)


# ============================================================
# 11. In-degree
# ============================================================

in_degree = (
    edges
    .groupBy("author2")
    .count()
    .withColumnRenamed(
        "author2",
        "author"
    )
    .withColumnRenamed(
        "count",
        "in_degree"
    )
)


# ============================================================
# 12. Top 5 by in-degree
# ============================================================

print("\n==========================================")
print("TOP 5 AUTHORS BY IN-DEGREE")
print("==========================================")

(
    in_degree
    .orderBy(
        col("in_degree").desc()
    )
    .show(
        5,
        truncate=False
    )
)


# ============================================================
# 13. Top 5 by out-degree
# ============================================================

print("\n==========================================")
print("TOP 5 AUTHORS BY OUT-DEGREE")
print("==========================================")

(
    out_degree
    .orderBy(
        col("out_degree").desc()
    )
    .show(
        5,
        truncate=False
    )
)


# ============================================================
# 14. Combined degree
# ============================================================

degree_df = (
    in_degree
    .join(
        out_degree,
        on="author",
        how="outer"
    )
    .fillna(0)
    .withColumn(
        "total_degree",
        col("in_degree") +
        col("out_degree")
    )
)


print("\n==========================================")
print("TOP AUTHORS BY TOTAL DEGREE")
print("==========================================")

degree_df.orderBy(
    col("total_degree").desc()
).show(
    10,
    truncate=False
)


# ============================================================
# 15. Stop Spark
# ============================================================

spark.stop()
