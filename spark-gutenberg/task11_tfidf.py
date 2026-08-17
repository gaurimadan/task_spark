from pyspark.sql import SparkSession
import re
import math
from collections import Counter


# ============================================================
# 1. Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gutenberg TF-IDF Similarity")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext


# ============================================================
# 2. Dataset
# ============================================================

DATASET_PATH = "/home/gauri/Downloads/D184MB"


# ============================================================
# 3. Stop words
# ============================================================

STOP_WORDS = set("""
a about above after again against all am an and any are as at be because
been before being below between both but by could did do does doing down
during each few for from further had has have having he her here hers
herself him himself his how i if in into is it its itself just me more
most my myself no nor not of off on once only or other our ours ourselves
out over own same she should so some such than that the their theirs them
themselves then there these they this those through to too under until up
very was we were what when where which while who whom why will with you
your yours yourself yourselves
""".split())


# ============================================================
# 4. Tokenization
# ============================================================

def tokenize(text):

    # Remove Gutenberg header
    text = re.sub(
        r"(?is).*?\*\*\*\s*start of.*?\*\*\*",
        " ",
        text
    )

    # Remove Gutenberg footer
    text = re.sub(
        r"(?is)\*\*\*\s*end of.*",
        " ",
        text
    )

    # Lowercase
    text = text.lower()

    # Extract words
    words = re.findall(
        r"[a-z]{2,}",
        text
    )

    # Remove stopwords
    words = [
        word
        for word in words
        if word not in STOP_WORDS
    ]

    return words


# ============================================================
# 5. Process ONE book
# ============================================================

def process_book(record):

    filepath, text = record

    filename = filepath.split("/")[-1]

    words = tokenize(text)

    counts = Counter(words)

    total_words = sum(counts.values())

    if total_words == 0:
        return filename, {}, set()

    # Term Frequency
    tf = {
        word: count / total_words
        for word, count in counts.items()
    }

    # Set of words appearing in this document
    document_words = set(counts.keys())

    return filename, tf, document_words


# ============================================================
# 6. Read books using Spark
# ============================================================

print("\nReading Gutenberg dataset...")

books_rdd = sc.wholeTextFiles(
    DATASET_PATH,
    minPartitions=8
)

print(
    "Total books:",
    books_rdd.count()
)


# ============================================================
# 7. Process books in parallel
# ============================================================

print("\nProcessing books with Spark...")

processed_rdd = (
    books_rdd
    .map(process_book)
)

# Only 425 document-level results will be collected
documents = processed_rdd.collect()

print(
    "Processed documents:",
    len(documents)
)


# ============================================================
# 8. Calculate document frequency LOCALLY
# ============================================================

print("\nCalculating document frequencies...")

document_frequency = Counter()

for filename, tf, document_words in documents:

    for word in document_words:
        document_frequency[word] += 1


number_of_documents = len(documents)

print(
    "Unique vocabulary:",
    len(document_frequency)
)


# ============================================================
# 9. Calculate IDF
# ============================================================

print("\nCalculating IDF...")

idf = {}

for word, df in document_frequency.items():

    idf[word] = math.log(
        number_of_documents / (df + 1)
    )


# ============================================================
# 10. Create TF-IDF vectors
# ============================================================

print("\nCreating TF-IDF vectors...")

tfidf_documents = {}

for filename, tf, document_words in documents:

    vector = {}

    for word, tf_value in tf.items():

        vector[word] = (
            tf_value *
            idf.get(word, 0.0)
        )

    tfidf_documents[filename] = vector


# ============================================================
# 11. Get 10.txt
# ============================================================

if "10.txt" not in tfidf_documents:

    print("\nERROR: 10.txt was not found.")

    spark.stop()
    exit(1)


target_vector = tfidf_documents["10.txt"]


# ============================================================
# 12. Calculate target norm
# ============================================================

target_norm = math.sqrt(
    sum(
        value * value
        for value in target_vector.values()
    )
)


# ============================================================
# 13. Cosine similarity
# ============================================================

print("\nCalculating cosine similarities...")

results = []

for filename, vector in tfidf_documents.items():

    if filename == "10.txt":
        continue

    vector_norm = math.sqrt(
        sum(
            value * value
            for value in vector.values()
        )
    )

    if target_norm == 0 or vector_norm == 0:

        similarity = 0.0

    else:

        # Iterate over smaller dictionary
        if len(target_vector) < len(vector):

            dot_product = sum(
                value *
                vector.get(word, 0.0)
                for word, value
                in target_vector.items()
            )

        else:

            dot_product = sum(
                target_vector.get(word, 0.0) *
                value
                for word, value
                in vector.items()
            )

        similarity = (
            dot_product /
            (target_norm * vector_norm)
        )

    results.append(
        (filename, similarity)
    )


# ============================================================
# 14. Top 5
# ============================================================

results.sort(
    key=lambda x: x[1],
    reverse=True
)

print("\n==========================================")
print("TOP 5 BOOKS SIMILAR TO 10.txt")
print("==========================================")

for filename, score in results[:5]:

    print(
        f"{filename:20s} {score:.6f}"
    )


# ============================================================
# 15. Stop Spark
# ============================================================

spark.stop()
