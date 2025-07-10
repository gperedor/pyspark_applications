import datetime

from pyspark.testing import assertDataFrameEqual

from pyspark_applications.etl.crm_analytics import (
    CRMAnalyticsPipeline,
    CRMSalesPipelinePipeline,
    CRMSalesTeamPipeline,
)


def test_crm_analytics_pipeline(test_data_dir, spark):
    import pyspark.sql.functions as _f

    pipeline = CRMAnalyticsPipeline(
        spark,
        CRMSalesPipelinePipeline(
            spark, str(test_data_dir / "crm_analytics/sales_pipeline.csv")
        ),
        CRMSalesTeamPipeline(
            spark, str(test_data_dir / "crm_analytics/sales_teams.csv")
        ),
    )

    actual = (
        pipeline.monthly_sales_growth_by_regional_office()
        .orderBy("regional_office", "sale_year_month")
        .filter(_f.col("regional_office") == "Central")
    )

    expected = spark.createDataFrame(
        [
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 3, 1, 0, 0),
                "sales_monthly": 347988,
                "sales_previous_month": None,
                "sales_monthly_growth": None,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 4, 1, 0, 0),
                "sales_monthly": 212275,
                "sales_previous_month": 347988,
                "sales_monthly_growth": -0.39,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 5, 1, 0, 0),
                "sales_monthly": 335778,
                "sales_previous_month": 212275,
                "sales_monthly_growth": 0.582,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 6, 1, 0, 0),
                "sales_monthly": 409268,
                "sales_previous_month": 335778,
                "sales_monthly_growth": 0.219,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 7, 1, 0, 0),
                "sales_monthly": 293349,
                "sales_previous_month": 409268,
                "sales_monthly_growth": -0.283,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 8, 1, 0, 0),
                "sales_monthly": 373289,
                "sales_previous_month": 293349,
                "sales_monthly_growth": 0.273,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 9, 1, 0, 0),
                "sales_monthly": 386564,
                "sales_previous_month": 373289,
                "sales_monthly_growth": 0.036,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 10, 1, 0, 0),
                "sales_monthly": 294401,
                "sales_previous_month": 386564,
                "sales_monthly_growth": -0.238,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 11, 1, 0, 0),
                "sales_monthly": 295680,
                "sales_previous_month": 294401,
                "sales_monthly_growth": 0.004,
            },
            {
                "regional_office": "Central",
                "sale_year_month": datetime.datetime(2017, 12, 1, 0, 0),
                "sales_monthly": 397701,
                "sales_previous_month": 295680,
                "sales_monthly_growth": 0.345,
            },
        ]
    )

    assertDataFrameEqual(actual, expected, ignoreColumnOrder=True)
