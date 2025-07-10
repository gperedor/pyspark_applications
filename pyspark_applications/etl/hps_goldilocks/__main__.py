import argparse
import json
import pathlib
import sys
from pprint import pprint

from pyspark.sql import SparkSession

LOGGER_NAME = pathlib.Path(sys.modules["__main__"].__file__ or __file__).parent.name


def main(spark: SparkSession, input_path_str, ranks_str, output_path_str=None):
    from pyspark_applications.etl.hps_goldilocks import ColumnBasedPartitioningPipeline

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(LOGGER_NAME)

    ranks = [int(n) for n in ranks_str.split(",")]

    logger.info("Computing rank statistics for DataFrame")
    result_dict = ColumnBasedPartitioningPipeline(
        spark, input_path_str, ranks
    ).get_rank_statistics(4)
    if output_path_str:
        spark.createDataFrame([result_dict]).write.mode("overwrite").json(
            output_path_str
        )
    else:
        pprint(json.dumps(result_dict))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="hps_goldilocks",
        description="Computes rank statistics for the given dataframe",
    )
    parser.add_argument("filename", help="Path to input file")
    parser.add_argument(
        "rank_statistics", help="Rank statistics separated by comma e.g 1,25,500"
    )
    parser.add_argument("-o", "--output-file", help="Output file to write output to")

    args = parser.parse_args()

    spark = SparkSession.builder.master("local").getOrCreate()

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

    main(spark, args.filename, args.rank_statistics, args.output_file)
