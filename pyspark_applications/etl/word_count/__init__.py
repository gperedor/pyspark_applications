import re

import pyspark.sql.functions as _f


class WordCountPipelineDF:
    "A DataFrame-based implementation of a simple most-frequent-words program"

    def __init__(self, spark, resource_path):

        df = spark.read.text(resource_path)
        df = df.withColumn("word_array", _f.split(_f.col("value"), "[ ,\\.;]"))
        df = df.withColumn("word", _f.explode("word_array"))
        df = df.where(_f.col("word") != "").select("word")

        self.df = df

    def top_most_frequent_words(self, n):
        n_rows = (
            self.df.groupBy("word")
            .agg(_f.count("word").alias("cnt"))
            .orderBy(_f.col("cnt").desc())
            .take(n)
        )
        return {r.word: r.cnt for r in n_rows}


class WordCountPipelineRDD:
    """
    An RDD implementation of a simple most-frequent-words program

    No meaningful advantages over the DF implementation, a greedy algorithm is
    possible using treeAggregate, to trim key space early, but
    that is way overkill for this example
    """

    def __init__(self, spark, resource_path):
        sc = spark.sparkContext

        text_rdd = sc.textFile(resource_path)
        self.words_rdd = text_rdd.flatMap(lambda r: re.split("[, \\.\r\n]", r)).filter(
            lambda r: r != ""
        )

    def top_most_frequent_words(self, n):
        # This could have been a .countByValue
        # What we did well to avoid was a grouByKey, the
        # aggregateByKey method leverages local aggregation to
        # make for smaller shuffles and lessened memory usage
        return dict(
            self.words_rdd.map(lambda word: (word, 1))
            .aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y)
            .sortBy(lambda r: r[1], ascending=False)
            .take(n)
        )
