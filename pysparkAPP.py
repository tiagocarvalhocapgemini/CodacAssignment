#Essential imports for the app
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DateType, BooleanType, StructType, StructField, LongType

#Spark Session with app name
spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("pysparkAPP")\
    .getOrCreate()

#logging module
LOGGER = logging.getLogger(__file__)

#Input Arguments
def input_arguments() -> str:
    """
    This function takes the 3 arguments required for the app: 2 path to datasets and a list of countries to preserve
    The paths can be copied and pasted from any directory and the function prepares them to be compatible with spark.read.csv
    The list can be dynamic but restricted to the countries in the original data

    """

    clients_path = input("Enter the path to the clients data with no spaces: ")
    financial_path = input("Enter the path to the financial data with no spaces: ")

    LOGGER.warning("Countries available to choose from: United States,Netherlands,France,United Kingdom")
    list_of_countries = input("Enter the countries separated by a comma: ")
    list_of_countries = list_of_countries.split(",")

    return clients_path, financial_path, list_of_countries

def raw_data(clients_path: str,financial_path: str, spark: object) -> object:

    """
    The function takes 2 inputs: 2 paths to our files that must exist locally
    Here the schema for both clients and financial data is defined since it is well known from the problem setting
    With the schema and options well defined, the function returns 2 Pyspark Dataframes
    
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


    clients_df = spark.read.format("csv").option("header", True).schema(clients_schema).load(clients_path)
    financial_df = spark.read.format("csv").option("header", True).schema(financial_schema).load(financial_path)

    return clients_df, financial_df

def filter_dataframes(clients_df: object, list_of_countries: list) -> object:

    """
    Function that applies the filters required by the problem: limited countries
    """
    clients_by_country_df = clients_df.filter((clients_df.country).isin(list_of_countries))

    return clients_by_country_df

def join_dataframes(clients_by_country_df: object, financial_df: object) -> object:

    """
    Function to join the clients data together with the financial data on id column
    """
    client_data_df = clients_by_country_df.join(financial_df, on = "id")
    return client_data_df

def final_client_df(client_data_df: object) -> object:

    """
    Takes the client data and removes the columns with sensitive information
    It also renames the columns according to the problem requirements
    """

    client_data_df = client_data_df.drop("first_name", "last_name", "country", "cc_n")
    client_data_df = client_data_df.withColumnRenamed("cc_t", "credit_card_type")\
                                    .withColumnRenamed("btc_a", "bitcoin_address")\
                                    .withColumnRenamed("id", "client_identifier")
    return client_data_df

def create_csv(final_df: object) -> object:

    """
    This function finalizes the project by creating the CSV file to be uploaded into Github.
    """
    final_df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .format("com.databricks.spark.csv")\
        .option("header",True)\
        .save("clients_data.csv")


if __name__ == "__main__":

    LOGGER.warning("Starting the application")
    clients_path, financial_path, list_of_countries = input_arguments()
    
    LOGGER.warning("Preparing the data.")
    clients_df, financial_df = raw_data(clients_path=clients_path,financial_path=financial_path, spark = spark)

    LOGGER.warning("Filtering the data to fit the requirements")
    clients_by_country_df = filter_dataframes(clients_df=clients_df, list_of_countries=list_of_countries)
    joint_df = join_dataframes(clients_by_country_df=clients_by_country_df, financial_df= financial_df)
    final_df =final_client_df(client_data_df=joint_df)
    
    LOGGER.warning("Creating the file and finalizing.")
    create_csv(final_df)
    
    LOGGER.warning("All tasks completed successfully")