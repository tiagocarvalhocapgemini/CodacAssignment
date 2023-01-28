from src import pysparkAPP
import logging
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from chispa.schema_comparer import assert_schema_equality

spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("chispatesting")\
        .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

def test_paths_and_filters():

    """
    Testing the function that provides the paths for the local files and the filters to be applied
    """

    paths_test = ["../Data/dataset_one.csv","../Data/dataset_two.csv"]
    filters_test = ["United Kingdom", "Netherlands"]

    #The number of parameters is 2 because in the problem there are 2 countries and 2 files
    paths_, filters_ = pysparkAPP.paths_and_filters(2, 2)

    assert paths_ == paths_test
    assert filters_ == filters_test


def test_raw_data():
  """
  Testing the function that retreives the raw data from the csv files
  It tests if the schema definition is correct and if the select columns have the correct data types, which are important for later
  """

  #Create a test DF
  data = [("Anna", 133322433322234), ("Michael", 133322433325543)]
  test_df = spark.createDataFrame(data, ["first_name", "cc_n"])

  path1 = "../Data/dataset_one.csv"
  path2 = "../Data/dataset_two.csv"

  #Values from the real function
  df1, df2 = pysparkAPP.raw_data(path1, path2, spark)

  #Assertion 1 
  s1 = test_df.schema["first_name"].dataType
  s2 = df1.schema["first_name"].dataType
  assert_schema_equality(s1, s2)

  #Assertion 2 
  s1 = test_df.schema["cc_n"].dataType
  s2 = df2.schema["cc_n"].dataType
  assert_schema_equality(s1, s2)


def test_filter_dataframes():
  """
  Check if the filters applied work, returning only the active users and the desired countries
  """

  #Test data
  raw_data_clients = [(1, "Anna", "United Kingdom"), (2, "Michael", "Portugal"), (3, "Tiago", "Netherlands"), (4, "Adrian", "Netherlands"),
            (5, "John", "United Kingdom"), (6, "Jane", "France"), (7, "Mark", "Netherlands")]

  clients_dataframe = spark.createDataFrame(raw_data_clients, ["id", "first_name", "country"])

  parameters = ["Netherlands", "United Kingdom"]

  #Applying the real function to the test data
  clients_by_country_df = pysparkAPP.filter_dataframes(clients_dataframe, parameters, "country")

  #Filtered data to test
  filtered_data_clients = [(1, "Anna", "United Kingdom"), (3, "Tiago", "Netherlands"), (4, "Adrian", "Netherlands"),
                    (5, "John", "United Kingdom"), (7, "Mark", "Netherlands")]


  test_clients_df = spark.createDataFrame(filtered_data_clients, ["id", "first_name", "country"])

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

  test_joined_dfs = pysparkAPP.join_dataframes(clients_dataframe, financial_df, "id")


  joined_data = [(1, "Anna", "Poland", "1wjtPamAZ", "visa"), (2, "Michael", "Portugal", "rh8HWfrrY", "amex"), (3, "Tiago", "Poland", "CzB8LXVBTHU", "jcb")]
  joined_data_df = spark.createDataFrame(joined_data, ["id", "first_name", "country","bitcoin_address", "card type"])

  assert_df_equality(joined_data_df, test_joined_dfs)


def test_drop_columns():

  """
  Testing the function for the removal of the columns
  """

  data = [(1, "Mark", "Zuckerberg", "rh8HWfrrY","abc@gmail.com", "USA", "amex", 344512232),
          (2, "Emilia", "Clark", "1wjtPamAZ","csdd@gmail.com","France", "visa", 333345213)]

  columns = ['id', 'first_name', 'last_name', 'btc_a','email', 'country', 'cc_t', 'cc_n']

  test_df = spark.createDataFrame(data, columns)
  columns_to_remove = ["first_name", "last_name", "country", "cc_n"]

  filtered_test_df = pysparkAPP.drop_columns(test_df, columns_to_remove)

  cleaned_data = [(1, "rh8HWfrrY", "abc@gmail.com", 'amex'), (2, "1wjtPamAZ", "csdd@gmail.com", 'visa')]

  cleaned_data_df = spark.createDataFrame(cleaned_data, ['id', 'btc_a', 'email', "cc_t"])

  assert_df_equality(cleaned_data_df, filtered_test_df)


def test_rename_columns():

  """
  Control to check if the columns were renamed properly
  """

  data = [(1, "rh8HWfrrY", "abc@gmail.com", 'amex'), (2, "1wjtPamAZ", "csdd@gmail.com", 'visa')]

  df_wrong_columns = spark.createDataFrame(data, ['id', 'btc_a', 'email', "cc_t"])

  new_names = {"cc_t": "credit_card_type", 
                "btc_a": "bitcoin_address", 
                "id": "client_identifier"}

  corrected_df = pysparkAPP.rename_columns(df_wrong_columns, new_names)

  df_correct_columns = spark.createDataFrame(data, ['client_identifier', 'bitcoin_address', 'email', "credit_card_type"])

  assert_df_equality(df_correct_columns, corrected_df)