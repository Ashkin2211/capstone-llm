import argparse
import logging
from pyspark.sql import SparkSession
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
from pyspark.sql.functions import explode, collect_list
import os

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def clean(spark: SparkSession, environment: str, tag: str):
    # df_q = spark.read.json("questions.json")
    df_q = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json")
    df_q_selected = (
        df_q.select(explode("items").alias("q"))
        .select("q.question_id","q.title","q.body")
        .withColumnsRenamed(
            {"body" : "Questions"}))
    
    # df_a = spark.read.json("answers.json")
    df_a = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json")
    df_a_selected = (
        df_a.select(explode("items").alias("a"))
        .select("a.question_id","a.body")
        .withColumnsRenamed(
            {"body" : "Answers"}))

    df_joined = df_q_selected.join(
        df_a_selected,
        on="question_id",
        how="inner")

    # df_q.printSchema()
    # df_a.printSchema()
    # df_q_selected.show(5)
    # df_a_selected.show(5)
    # df_joined.show(5)

    df_per_question = (df_joined.
    groupBy("question_id", "title", "Questions").
    agg(collect_list("Answers").alias("Answers")))

    df_per_question.show(5)
    df_per_question.printSchema()

    (df_per_question.write
    .json("s3a://dataminded-academy-capstone-llm-data-us/cleaned/Ashkin2211/dbt"))

    return 0

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()

