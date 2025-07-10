from pyspark_applications.etl.hps_goldilocks import ColumnBasedPartitioningPipeline


def test_single_rank_statistic(test_data_dir, spark):
    actual = ColumnBasedPartitioningPipeline(
        spark, str(test_data_dir / "goldilocks.csv"), [1]
    ).get_rank_statistics()

    expected = {"Happiness": [2], "Niceness": [100], "Sweetness": [0]}

    assert actual == expected
