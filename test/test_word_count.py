from pyspark_applications.etl.word_count import (
    WordCountPipelineDF,
    WordCountPipelineRDD,
)


def test_word_count_df(test_data_dir, spark):
    actual = WordCountPipelineDF(
        spark, str(test_data_dir / "word_count.txt")
    ).top_most_frequent_words(10)

    expected = {
        "ipsum": 8,
        "and": 8,
        "Lorem": 7,
        "the": 7,
        "a": 5,
        "to": 5,
        "of": 5,
        "text": 4,
        "in": 4,
        "it": 4,
    }

    assert actual == expected


def test_word_count_rdd(test_data_dir, spark):
    actual = WordCountPipelineRDD(
        spark, str(test_data_dir / "word_count.txt")
    ).top_most_frequent_words(10)

    expected = {
        "ipsum": 8,
        "and": 8,
        "Lorem": 7,
        "the": 7,
        "a": 5,
        "to": 5,
        "of": 5,
        "text": 4,
        "in": 4,
        "it": 4,
    }

    assert actual == expected
