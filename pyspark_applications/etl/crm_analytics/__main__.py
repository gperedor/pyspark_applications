import argparse
import pathlib
import sys

from pyspark.sql import SparkSession

LOGGER_NAME = pathlib.Path(sys.modules["__main__"].__file__ or __file__).parent.name


def main(
    spark: SparkSession,
    sales_pipeline_path_str,
    sales_team_path_str,
    output_path_str=None,
):
    from pyspark_applications.etl.crm_analytics import (
        CRMAnalyticsPipeline,
        CRMSalesPipelinePipeline,
        CRMSalesTeamPipeline,
    )

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(LOGGER_NAME)

    logger.info("Computing rank statistics for DataFrame")
    pipeline = CRMAnalyticsPipeline(
        spark,
        CRMSalesPipelinePipeline(spark, sales_pipeline_path_str),
        CRMSalesTeamPipeline(spark, sales_team_path_str),
    )

    monthly_growth_per_office = pipeline.monthly_sales_growth_by_regional_office()

    if output_path_str:
        monthly_growth_per_office.write.mode("overwrite").json(output_path_str)
    else:
        # In manual tests, caching isn't any quicker than computing twice,
        # once for count and once for show, but let's leave it up to Spark
        # for cases where it would be costly
        monthly_growth_per_office.cache()
        monthly_growth_per_office.orderBy("regional_office", "sale_year_month").show(
            monthly_growth_per_office.count()
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="hps_goldilocks",
        description="Computes rank statistics for the given dataframe",
    )
    parser.add_argument(
        "sales_pipeline_filename", help="Path to sales pipeline input file"
    )
    parser.add_argument("sales_team_filename", help="Path to sales team input file")

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

    main(
        spark, args.sales_pipeline_filename, args.sales_team_filename, args.output_file
    )
