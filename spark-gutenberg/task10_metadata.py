from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    desc,
    avg,
    length,
    trim
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Gutenberg Metadata Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Dataset
# --------------------------------------------------

DATASET_PATH = "/home/gauri/Downloads/D184MB"

# --------------------------------------------------
# Read complete files
# --------------------------------------------------

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

print("\n========== BOOKS DATAFRAME ==========")

books_df.show(
    5,
    truncate=80
)

print(
    "Total books:",
    books_df.count()
)

# --------------------------------------------------
# Metadata extraction
# --------------------------------------------------

books_metadata = (
    books_df

    .withColumn(
        "title",
        trim(
            regexp_extract(
                col("text"),
                r"(?im)^Title:\s*(.+)$",
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

    .withColumn(
        "language",
        trim(
            regexp_extract(
                col("text"),
                r"(?im)^Language:\s*(.+)$",
                1
            )
        )
    )

    .withColumn(
        "encoding",
        trim(
            regexp_extract(
                col("text"),
                r"(?im)^Character set encoding:\s*(.+)$",
                1
            )
        )
    )
)

# --------------------------------------------------
# Display metadata
# --------------------------------------------------

print("\n========== EXTRACTED METADATA ==========")

books_metadata.select(
    "file_name",
    "title",
    "release_date",
    "language",
    "encoding"
).show(
    20,
    truncate=False
)

# --------------------------------------------------
# Release year
# --------------------------------------------------

books_metadata = books_metadata.withColumn(
    "release_year",
    regexp_extract(
        col("release_date"),
        r"(\d{4})",
        1
    )
)

print("\n========== BOOKS RELEASED EACH YEAR ==========")

books_per_year = (
    books_metadata
    .filter(col("release_year") != "")
    .groupBy("release_year")
    .count()
    .orderBy("release_year")
)

books_per_year.show(
    100,
    truncate=False
)

# --------------------------------------------------
# Most common language
# --------------------------------------------------

print("\n========== MOST COMMON LANGUAGES ==========")

language_counts = (
    books_metadata
    .filter(
        trim(col("language")) != ""
    )
    .groupBy("language")
    .count()
    .orderBy(desc("count"))
)

language_counts.show(
    20,
    truncate=False
)

# --------------------------------------------------
# Average title length
# --------------------------------------------------

print("\n========== AVERAGE TITLE LENGTH ==========")

avg_title_length = (
    books_metadata
    .filter(
        trim(col("title")) != ""
    )
    .select(
        avg(
            length(trim(col("title")))
        ).alias("average_title_length")
    )
)

avg_title_length.show()

# --------------------------------------------------
# Missing metadata
# --------------------------------------------------

print("\n========== MISSING METADATA ==========")

for field in [
    "title",
    "release_date",
    "language",
    "encoding"
]:

    missing = (
        books_metadata
        .filter(
            col(field).isNull()
            |
            (trim(col(field)) == "")
        )
        .count()
    )

    print(
        field,
        "missing:",
        missing
    )

# --------------------------------------------------
# Stop
# --------------------------------------------------

spark.stop()
