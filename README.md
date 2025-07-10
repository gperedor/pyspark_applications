# PYSPARK APPLICATIONS

Some sample problems showcasing PySpark pipelines written as Python
applications.

## DEPENDENCIES

Create a suitable virtual environment (e.g. [venv](https://docs.python.org/3/library/venv.html)),
activate, and issue

```sh
python3 -m pip install -r requirements-dev.txt -r requirements-test.txt
```

## TESTING

Issue

```sh
make test
```

## BUILDING

Issue

```sh
make install
```

## RUNNING

The following pipelines are included:

1. pyspark_applications.etl.hps_goldilocks: an implementation of the column-based
partitioning solution for the High Performance Spark goldilocks problem
2. pyspark_applications.etl.word_count: a simple top-n most frequent word
counting program, with both an RDD and a DataFrame backend

### HELP

A basic usage blurb is printed by running the module with `-h` switch, e.g.

```sh
python3 -m pyspark_applications.etl.hps_goldilocks -h
```

### STAND-ALONE

To run the pipelines as stand-alone programs, run them as python modules with
their input data, e.g. the test fixtures, as follows:

```sh
python3 -m pyspark_applications.etl.hps_goldilocks test/data/hps_goldilocks.csv 1,2
```

### SUBMIT TO LOCAL SPARK INSTANCE

To run the pipelines submitted in client mode as a job to a local spark
instance, perform the build step above and issue a `spark-submit` command
including the `whl` file as a `--py-files` option and including the
data file as a `--files` input.

```sh
spark-submit --master local \
             --files test/data/goldilocks.csv \
             --py-files dist/pyspark_applications-0.0.1-py3-none-any.whl \
             pyspark_applications/etl/hps_goldilocks/__main__.py \
             -o /tmp/out.json test/data/goldilocks.csv \
             1,10 2&>/dev/null
```

## ARCHITECTURAL NOTES

### WHY WRITE SPARK PIPELINES AS PYTHON APPLICATIONS

A hybrid object-oriented and functional approach, much in the spirit
of Scala Spark, affords options to organize the
code base in a maintainable manner, as well as greater ease of
unit testing, and allows for cleaner composition of pipelines within
the same Spark application, without the need for
external orchestration and costly writes and reads as
the approach of orchestrating functions in Airflow
would result in.

### DOWNSIDES OF THIS APPROACH

More generally, using software-engineering techniques necessitates knowledge of
them, data engineering teams may be comprised of members without such a
background.

More specifically, Python module discovery in a Spark application needs
some accommodation, like including an otherwise unnecessary `__init__.py`
file at every level of the hierarchy, and some ceremony in
the `__main__.py` files to include the dependency of the class
hierarchy.
