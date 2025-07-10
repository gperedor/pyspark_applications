class ColumnBasedPartitioningPipeline:
    """
    Computes rank statistics on a per-column basis on a dataframe.

    Follows one of the proposed solutions for the High Performance Spark
    Goldilocks problem

    Parallelizes workload on column-level basis by splitting dataframe into a column-value
    entries RDD and sorting
    """

    def __init__(self, spark, resource_path, target_ranks):
        self.df = spark.read.csv(resource_path, header=True, inferSchema=True).drop(
            "Panda Name"
        )
        self.target_ranks = target_ranks

    @staticmethod
    def make_partition_func(rdd, partitions=None):
        n = partitions or rdd.getNumPartitions()
        return lambda key: abs(key[0]) % n

    @staticmethod
    def key_by_dimension(df):
        cols = df.columns
        enumerated = list(enumerate(cols))
        return df.rdd.flatMap(
            lambda row: [((ix + 1, row[ix]), 1) for (ix, _) in enumerated]
        )

    @staticmethod
    def rank_statistics_map(seq):
        statistics = {}
        for key, val in seq:
            vals = statistics.get(key, [])
            vals.append(val)
            statistics[key] = vals
        return dict(sorted(statistics.items()))

    def make_rank_partition(self):
        # avoids including self in fn to use in Spark function
        # parent object can't be serialized for it
        ranks = self.target_ranks

        def rank_partition(it):
            current_column_index = -1
            rank = 1
            for (col_index, value), _ in it:
                if col_index != current_column_index:
                    current_column_index = col_index
                    rank = 1
                else:
                    rank += 1
                if rank in ranks:
                    yield (col_index, value)

        return rank_partition

    def find_rank_statistics_partitioned_sort(self, df, partitions):
        # Partitions rows by exploding the dataframe as an RDD
        # of tuples of column values,
        # to leverage the `repartitionAndSortWithinPartitions`
        # method and then filter rows corresponding to the
        # rank statistics by position within the sorted
        # partitions
        pair_rdd = ColumnBasedPartitioningPipeline.key_by_dimension(df)
        sorted_rdd = pair_rdd.repartitionAndSortWithinPartitions(
            partitionFunc=ColumnBasedPartitioningPipeline.make_partition_func(
                pair_rdd, partitions
            ),
            numPartitions=partitions,
        )
        filtered_for_target_indexes = sorted_rdd.mapPartitions(
            self.make_rank_partition(), preservesPartitioning=True
        )
        return filtered_for_target_indexes

    def get_rank_statistics(self, num_partitions=1):
        result = ColumnBasedPartitioningPipeline.rank_statistics_map(
            self.find_rank_statistics_partitioned_sort(
                self.df, num_partitions
            ).collect()
        )
        return {col_name: result[ix + 1] for ix, col_name in enumerate(self.df.columns)}
