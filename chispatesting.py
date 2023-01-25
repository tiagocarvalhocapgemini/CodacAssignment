from chispa import assert_df_equality
from chispa.schema_comparer import assert_schema_equality
from pysparkAPP import raw_data, filter_dataframes, join_dataframes, final_client_df
from pyspark.sql import SparkSession
import logging

spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("chispatesting")\
        .enableHiveSupport()\
        .getOrCreate()

logger = logging.getLogger(__file__)

path1 = "../CodacAssignment/data/dataset_one.csv"
path2 = "../CodacAssignment/data/dataset_two.csv"
list_countries = ["Poland", "France"]


def test_raw_data(path1, path2):

    """
    Testing the function that retreives the raw data from the csv files
    It tests if the schema definition is correct and if the select columns have the correct data types, which are important for later
    """

    #Create a test DF
    data = [("Anna", 133322433322234), ("Michael", 133322433325543)]
    test_df = spark.createDataFrame(data, ["first_name", "cc_n"])

    #Values from the real function
    df1, df2 = raw_data(path1, path2, spark)

    #Assertion 1 
    s1 = test_df.schema["first_name"].dataType
    s2 = df1.schema["first_name"].dataType
    assert_schema_equality(s1, s2)

    #Assertion 2 
    s1 = test_df.schema["cc_n"].dataType
    s2 = df2.schema["cc_n"].dataType
    assert_schema_equality(s1, s2)

def test_filter_dataframes(list_of_countries):
  """
  Check if the filters applied work, returning only the active users and the desired countries
  """

  #Test data
  raw_data_clients = [(1, "Anna", "Poland"), (2, "Michael", "Portugal"), (3, "Tiago", "Poland"), (4, "Adrian", "France"),
            (5, "John", "United States"), (6, "Jane", "France"), (7, "Mark", "Poland")]

  clients_dataframe = spark.createDataFrame(raw_data_clients, ["id", "firt_name", "country"])

  #Applying the real function to the test data
  clients_by_country_df = filter_dataframes(clients_dataframe, list_countries)

  #Filtered data to test
  filtered_data_clients = [(1, "Anna", "Poland"), (3, "Tiago", "Poland"), (4, "Adrian", "France"),
                    (6, "Jane", "France"), (7, "Mark", "Poland")]


  test_clients_df = spark.createDataFrame(filtered_data_clients, ["id", "firt_name", "country"])

  #Chispa test assertion
  assert_df_equality(test_clients_df, clients_by_country_df)

def test_join_dataframes():

  """
  Join the dataframes on id column which must be present on both dataframes
  """

  #Test data
  raw_data_clients = [(1, "Anna", "Poland"), (2, "Michael", "Portugal"), (3, "Tiago", "Poland")]

  raw_data_financial = [(1, "1wjtPamAZ", "visa"), (2, "rh8HWfrrY", "amex"), (3, "CzB8LXVBTHU", "jcb")]
  
  clients_dataframe = spark.createDataFrame(raw_data_clients, ["id", "first_name", "country"])
  financial_df = spark.createDataFrame(raw_data_financial, ["id", "bitcoin_address", "card type"])

  test_joined_dfs = join_dataframes(clients_dataframe, financial_df)


  joined_data = [(1, "Anna", "Poland", "1wjtPamAZ", "visa"), (2, "Michael", "Portugal", "rh8HWfrrY", "amex"), (3, "Tiago", "Poland", "CzB8LXVBTHU", "jcb")]
  joined_data_df = spark.createDataFrame(joined_data, ["id", "first_name", "country","bitcoin_address", "card type"])

  assert_df_equality(joined_data_df, test_joined_dfs)

def test_final_client_df():

  """
  Testing the final filters which eliminate the sensitive information
  """

  data = [(1, "Mark", "Zuckerberg", "rh8HWfrrY","abc@gmail.com", "USA", "amex", 344512232),
          (2, "Emilia", "Clark", "1wjtPamAZ","csdd@gmail.com","France", "visa", 333345213)]

  columns = ['id', 'first_name', 'last_name', 'btc_a','email', 'country', 'cc_t', 'cc_n']

  test_df = spark.createDataFrame(data, columns)

  filtered_test_df = final_client_df(test_df)

  cleaned_data = [(1, "rh8HWfrrY", "abc@gmail.com", 'amex'), (2, "1wjtPamAZ", "csdd@gmail.com", 'visa')]

  cleaned_data_df = spark.createDataFrame(cleaned_data, ['client_identifier', 'bitcoin_address', 'email', "credit_card_type"])
  
  assert_df_equality(cleaned_data_df, filtered_test_df)

if __name__ == "__main__":
  test_raw_data(path1, path2)
  test_filter_dataframes(list_countries)
  test_join_dataframes()
  test_final_client_df()
  logger.warning("All Chispa tests successful")
