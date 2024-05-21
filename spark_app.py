from pyspark.sql import SparkSession, DataFrame, functions as f
import configparser as cp
from pyspark.sql.functions import countDistinct
import pyspark
import sys
import argparse
import os

def create_session(master, appName):
    spk = SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .getOrCreate()
    return spk

def filter_registered_events(df: DataFrame) -> DataFrame:
    return df.select('event', 'timestamp', 'initiator_id', 'channel').where("event == 'registered'").withColumnRenamed('timestamp', 'time')
    #Checking whether some columns i need e.g. channel exists in the data, if it throw an error, i can return a empty dataframe.  

def filter_app_load_events(df: DataFrame) -> DataFrame:
    return df.select('event', 'timestamp', 'initiator_id', 'device_type').where("event == 'app_loaded'").withColumnRenamed('timestamp', 'time')

def ParseMode(spark: str, inputBaseDir: str, outputBaseDir: str):
    """
    Parse mode

    The parse mode selects the app_load and registerd events, and saves them on a disk in the parquet format.
    """
    df = spark.read.json(inputBaseDir)

    registered = filter_registered_events(df)
    registered.write.mode('overwrite').parquet(outputBaseDir + "/registered/")
    # checking if outputBaseDir exists and is writable otherwise you raise an error. 
    
    app_loaded = filter_app_load_events(df)
    app_loaded.write.mode('overwrite').parquet(outputBaseDir + "/app_loaded/")


def create_week_and_year_columns(df: DataFrame, date_col_name: str, add_days: int=None) -> DataFrame:
    df = df.withColumn(date_col_name, f.to_date(f.col(date_col_name)))

    if add_days:
        df = df.withColumn('_delta_time', f.date_add(f.col(date_col_name), add_days))
        date_col_name = '_delta_time'

    df = df.withColumn('week_of_year', f.weekofyear(f.col(date_col_name))).\
        withColumn('year', f.year(f.col(date_col_name)))

    return df

def StaticMode(spark: str, outputBaseDir: str) -> float:
    """
    Static mode

    The metric represents the percentage of all users who loaded the application in the calendar week
    immediately after the registration week.
    """
    try:
        days_in_a_week = 7
        registered = spark.read.parquet(outputBaseDir + "/registered/")
        registered = create_week_and_year_columns(registered, 'time', days_in_a_week)

        app_loaded = spark.read.parquet(outputBaseDir + "/app_loaded/")
        app_loaded = create_week_and_year_columns(app_loaded, 'time')

    except pyspark.errors.exceptions.captured.AnalysisException as err:
        raise RuntimeError('Failed to load parquet files, make sure to run the ParseMode first') from err
    
    joined_table = registered.join(app_loaded, ['initiator_id','week_of_year','year'],'inner')

    users_loaded_count = round(joined_table.select(f.countDistinct("initiator_id")).collect()[0][0])
    total_registered_user_count = round(registered.select(f.countDistinct("initiator_id")).collect()[0][0])

    return users_loaded_count / total_registered_user_count


def main():

    props = cp.RawConfigParser()
    props.read("./resources/application.properties")
    props.get("dev", "execution.mode")
    inputBaseDir = props.get("dev", 'input.base.dir')
    outputBaseDir = props.get("dev", 'output.base.dir')
    spark_master = props.get("dev", 'spark.session.master')
    spark_app_name = props.get("dev", 'spark.session.appName')
    
    spark = create_session(spark_master, spark_app_name)

    if sys.argv[1].lower() == 'parsemode':
        ParseMode(spark, inputBaseDir, outputBaseDir)
        print("Parquet files are created!")

    if sys.argv[1].lower() == 'staticmode':
        percentage = StaticMode(spark, outputBaseDir)
        print("Metric: {:.0%}".format(percentage))


if __name__ == "__main__":
    main()