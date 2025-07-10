import argparse
import json
import pathlib
import sys
from pprint import pprint

from pyspark.sql import SparkSession

LOGGER_NAME = pathlib.Path(sys.modules["__main__"].__file__ or __file__).parent.name


def main(
    spark: SparkSession, input_path_str, top_n_str, output_path_str=None, rdd_impl=False
):
    from pyspark_applications.etl.word_count import (
        WordCountPipelineDF,
        WordCountPipelineRDD,
    )

    top_n = int(top_n_str)

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(LOGGER_NAME)

    if rdd_impl:
        logger.info("Using RDD backend")
        pipeline = WordCountPipelineRDD(spark, input_path_str)
    else:
        logger.info("Using DataFrame backend")
        pipeline = WordCountPipelineDF(spark, input_path_str)

    logger.info("Ranking words by frequency")
    result_dict = pipeline.top_most_frequent_words(top_n)
    if output_path_str:
        spark.createDataFrame([result_dict]).write.mode("overwrite").json(
            output_path_str
        )
    else:
        pprint(json.dumps(result_dict))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="word_count",
        description="Outputs top n most frequent words in a file",
    )
    parser.add_argument("filename", help="Path to input file")
    parser.add_argument("top_n", help="Number of top n words to rank")
    parser.add_argument("-o", "--output-file", help="Output file to write output to")
    parser.add_argument("-R", "--rdd", help="Use the RDD backend", action="store_true")

    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local")
        .config("spark.log.level", "INFO")
        .getOrCreate()
    )

    # Root logger-related configurations are not propagating, must be
    # a bug
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(LOGGER_NAME)

    logger.setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.INFO)

    # Running as a job submitted to a cluster, must add the project itself
    # to the Python path
    project_package = spark.sparkContext.getConf().get("spark.submit.pyFiles")
    if project_package is not None and len(project_package) > 0:
        spark.sparkContext.addPyFile(project_package)

    main(spark, args.filename, args.top_n, args.output_file, rdd_impl=args.rdd)
