import pandas as pd
import  pickle
import ast
from xgboost import XGBRegressor
from transform import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Read CSV from HDFS to Spark") \
    .config("spark.jars", "/home/hadoop/postgresql-42.7.4.jar") \
    .getOrCreate()


def return_spark_df():
    hdfs_client = InsecureClient('http://localhost:9870')

    with hdfs_client.read("/batch-layer/raw_data.csv") as reader:
        pandas_df = pd.read_csv(reader)
    
    #iteritems is removed from pandas 2.0
    #can them dong nay vao de fix loi
    pd.DataFrame.iteritems = pd.DataFrame.items


    spark_df = spark.createDataFrame(pandas_df)

    return spark_df

def spark_tranform():
    spark_df = return_spark_df()

    spark_df = spark_df.dropna()

    print('sucess------------------')
    # Convert columns to numeric types
    spark_df = spark_df.withColumn("Kích thước màn hình", spark_df["Kích thước màn hình"].cast("float"))
    spark_df = spark_df.withColumn("Ram", spark_df["Ram"].cast("float"))
    spark_df = spark_df.withColumn("Rom", spark_df["Rom"].cast("float"))
    spark_df = spark_df.withColumn("Dung lượng pin", spark_df["Dung lượng pin"].cast("float")) 
    
    spark_df = spark_df.withColumn("Hãng sản xuất_", spark_df["Hãng sản xuất_"].cast("int")) 
    spark_df = spark_df.withColumn("Primary", spark_df["Primary"].cast("float")) 
    spark_df = spark_df.withColumn("Ultra_Wide", spark_df["Ultra_Wide"].cast("float")) 
    spark_df = spark_df.withColumn("Telephoto", spark_df["Telephoto"].cast("float")) 
    

    # Load pre-trained XGBoost model
    model = pickle.load(open("/home/hadoop/Predict-Phone-Price/ML/best_model_fix.pkl", "rb"))

    # Assemble features for prediction
    feature_cols = ['Hãng sản xuất_', 'Rom', 'Ram', 'Kích thước màn hình', 'Dung lượng pin', 'Primary', 'Ultra_Wide', 'Telephoto']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled_df = assembler.transform(spark_df)

    ######################################################
    # Convert assembled Spark DataFrame to Pandas DataFrame
    assembled_pandas_df = assembled_df.toPandas().drop(columns=['features'])
    # Predict prices
    predictions = model.predict(assembled_pandas_df[feature_cols])

    # Convert predictions to a Pandas DataFrame
    predictions_df = pd.DataFrame(predictions, columns=["Giá"])

    # Add predicted prices as a new column to the assembled Pandas DataFrame
    assembled_pandas_df["Giá"] = predictions_df

    # Convert the assembled Pandas DataFrame back to a Spark DataFrame
    spark_df = spark.createDataFrame(assembled_pandas_df)

    print("data transformed successfully")
    return spark_df

# if __name__ == "__main__":
#     # Run the function
#     df = return_spark_df()

#     # Display the first few rows of the Spark DataFrame
#     df.show()

#     # Additional checks
#     print("Number of rows:", df.count())
#     print("Schema of DataFrame:")
#     df.printSchema()
    
#     spark_tranform().show()

