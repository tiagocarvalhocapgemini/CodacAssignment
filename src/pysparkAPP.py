#Imports
import logging
import argparse
from typing import Tuple
from logging.handlers import RotatingFileHandler
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType

#Spark Session with app nameyes
spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("pysparkAPP")\
    .config("spark.logConf", "true")\
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
def logs() -> logging.LogRecord:
    """"
    Printing out the logs as requested
    """
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(filename="pysparkAPP_logs.log", maxBytes=10000, backupCount=10)
    logger.addHandler(handler)

    return logger


def paths_and_filters(nr_paths: int, nr_parameters: int) -> Tuple[list, list]:
    """
    This function takes the 3 arguments required for the app: 2 path to datasets and a list of countries to preserve
    The paths can be copied and pasted from any directory and the function prepares them to be compatible with spark.read.csv
    The list can be dynamic but restricted to the countries in the original data

    :param nr_paths: number of paths to local files that we wish to provide
    :param nr_parameters: number of filters that will be applied to our data
    :return: tuple with 2 lists inside, the 1st one containing the paths to the data and the 2nd one providing the filters
    """

    parser = argparse.ArgumentParser(prog = "Codac Assignment with PySpark")

    parser.add_argument('--paths', type = str, nargs = nr_paths, 
                        default = ['../Data/dataset_one.csv', '../Data/dataset_two.csv'],
                         help = "Paths to the local files containing the data")

    parser.add_argument("--filters", type = str, nargs = nr_parameters, 
                        default = ["United Kingdom", "Netherlands"], 
                        help = "Filters to apply to our data")
    args = parser.parse_args()

    return args.paths, args.filters


def raw_data(path1: str, path2: str, spark: SparkSession) -> Tuple[SparkDataFrame, SparkDataFrame]:

    """
    The function takes 2 inputs: 2 paths to our files that must exist locally
    Here the schema for both clients and financial data is defined since it is well known from the problem setting

    :param path1: path to the clients data csv file
    :param path2: path to the financial data csv file
    :param spark: pyspark module SparkSession
    :returns: 2 Pyspark dataframes with the data from both files mentioned above
    """

    clients_schema = StructType([StructField("id", IntegerType(), True),
                                StructField("first_name", StringType(), True),
                                StructField("last_name", StringType(), True),
                                StructField("email", StringType(), True),
                                StructField("country", StringType(), True)])

    financial_schema = StructType([StructField("id", IntegerType(), True),
                                StructField("btc_a", StringType(), True),
                                StructField("cc_t", StringType(), True),
                                StructField("cc_n", LongType(), True)])


    clients_df = spark.read.format("csv").option("header", True).schema(clients_schema).load(path1)
    financial_df = spark.read.format("csv").option("header", True).schema(financial_schema).load(path2)

    return clients_df, financial_df


def filter_dataframes(df: SparkDataFrame, parameters: list, column: str) -> SparkDataFrame:

    """
    Function that applies the filters required by the problem: limited countries

    :param df: pyspark DataFrame
    :param parameters: the dataframe needs to be filtered and return only with the parameters desired
    :param column: the column we wish to apply the filter on
    :returns: a filtered pyspark Dataframe 
    """
    filtered_df = df.filter(col(column).isin(parameters))

    return filtered_df


def join_dataframes(df1: SparkDataFrame, df2: SparkDataFrame, column_name: str) -> SparkDataFrame:

    """
    Function to join the clients data together with the financial data on id column

    :param df1: pyspark Dataframe
    :param df2: pyspark Dataframe
    :param column_name: the column the 2 Dataframes are joined on
    :returns: one pyspark Dataframe joined on the specified column
    """
    joined_df = df1.join(df2, on = column_name)
    return joined_df


def drop_columns(df: SparkDataFrame, columns: list) ->  SparkDataFrame:
    """
    Function that, as the names says, drops columns from a pypark Dataframe as defined by the user

    :param df: pyspark DataFrame
    :param columns: list of columns to be dropped
    :returns: pyspark Dataframe
    """
    for column_name in columns:
        df = df.drop(column_name)

    return df


def rename_columns(df: SparkDataFrame, new_names: dict) -> SparkDataFrame:

    """
    It renames the columns according to the problem requirements

    :param df: pyspark Dataframe
    :param new_names: dictionary with the names to be replaced
    """
    for old_name,new_name in new_names.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df


def create_csv(df: SparkDataFrame, file_name: str) -> None:

    """
    This function finalizes the project by creating the CSV file to be uploaded into Github.

    :param df: pyspark DataFrame
    :param file_name: name of the csv file to save
    """
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .format("com.databricks.spark.csv")\
        .option("header",True)\
        .save(file_name)

if __name__ == "__main__":

    logger = logs()
    logger.info("Starting the application")
    paths, filters = paths_and_filters(2,2)

    logger.info("Retreiving the data from the local machine")
    client_df, financial_df = raw_data(paths[0], paths[1], spark)

    logger.info("Applying the filters")
    filtered_data_clients = filter_dataframes(client_df, filters, "country")

    logger.info("Joining the data")
    joined_df = join_dataframes(filtered_data_clients, financial_df, "id")

    logger.info("Removing sensitive information")
    df = drop_columns(joined_df, ["first_name", "last_name", "country", "cc_n"])

    new_names = {"cc_t": "credit_card_type", 
                "btc_a": "bitcoin_address", 
                "id": "client_identifier"}
    df_final = rename_columns(df, new_names)

    logger.info("Preparing the CSV file")
    create_csv(df_final, "clients_data.csv")

    logger.info("All tasks concluded successfully.")