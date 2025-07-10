from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class CRMSalesPipelinePipeline:
    SPARK_SCHEMA = StructType(
        [
            StructField("opportunity_id", StringType(), False),
            StructField("sales_agent", StringType(), False),
            StructField("product", StringType(), False),
            StructField("account", StringType(), True),
            StructField("deal_stage", StringType(), False),
            StructField("engage_date", DateType(), True),
            StructField("close_date", DateType(), True),
            StructField("close_value", IntegerType(), True),
        ]
    )

    def __init__(self, spark, sales_pipeline_resource_path):
        # parsing and validation could happen here, but we won't do it for this example,
        # we'll just specify the schema with FAILFAST
        self._df = (
            spark.read.option("mode", "FAILFAST")
            .option("header", "true")
            .schema(CRMSalesPipelinePipeline.SPARK_SCHEMA)
            .csv(sales_pipeline_resource_path)
        )

    @property
    def sales_data(self):
        return self._df


class CRMSalesTeamPipeline:

    SPARK_SCHEMA = StructType(
        [
            StructField("sales_agent", StringType(), False),
            StructField("manager", StringType(), False),
            StructField("regional_office", StringType(), False),
        ]
    )

    def __init__(self, spark, sales_team_resource_path):
        self._df = (
            spark.read.option("mode", "FAILFAST")
            .option("header", "true")
            .schema(CRMSalesTeamPipeline.SPARK_SCHEMA)
            .csv(sales_team_resource_path)
        )

    @property
    def sales_team_data(self):
        return self._df


class CRMAnalyticsPipeline:
    """
    Computes an example performance metric for sales teams,
    based off the Kaggle crm-sales-opportunities dataset
    """

    def __init__(self, spark, crm_sales_pipeline, crm_sales_team):
        # Assumptions:
        ## sales_data is a potentially large DF, hundreds of millions of rows
        ## sales_team_data is orders of magnitude smaller
        ### (e.g. 100.000 rows of 3 string columns, assuming string columns are of
        ### avg len of 20 char each, each char is 2 byte, we get (3 x 20 x 2) x 1e6
        ### 120MB, multiply by a constant factor for overhead, we're well under the 2GB
        ### limit for broadcast, and the join can be made locally
        import pyspark.sql.functions as _f

        self.enriched_sales_pipeline = crm_sales_pipeline.sales_data.join(
            _f.broadcast(crm_sales_team.sales_team_data), on="sales_agent", how="inner"
        ).drop(crm_sales_team.sales_team_data["sales_agent"])

    def _monthly_sales_by_regional_office(self):
        import pyspark.sql.functions as _f

        return (
            self.enriched_sales_pipeline.withColumn(
                "sale_year_month", _f.date_trunc("mon", "close_date")
            )
            .filter(_f.col("sale_year_month").isNotNull())
            .groupBy("regional_office", "sale_year_month")
            .agg(_f.sum("close_value").alias("sales_monthly"))
        )

    def monthly_sales_growth_by_regional_office(self):
        import pyspark.sql.functions as _f
        from pyspark.sql import Window

        monthly_sales = self._monthly_sales_by_regional_office()

        w = (
            Window()
            .partitionBy("regional_office")
            .orderBy(_f.col("sale_year_month").asc())
        )
        return (
            monthly_sales.orderBy(
                _f.col("regional_office"), _f.col("sale_year_month").asc()
            )
            .withColumn(
                "sales_previous_month", _f.lag("sales_monthly", offset=1).over(w)
            )
            .withColumn(
                "sales_monthly_growth",
                _f.expr(
                    "round((sales_monthly - sales_previous_month)/sales_previous_month, 3)"
                ),
            )
        )
