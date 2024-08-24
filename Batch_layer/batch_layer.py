from spark_tranformation import spark_tranform

from save_data_postgresql import  save_data


def batch_layer():
    data = spark_tranform()
    save_data(data)
    
if __name__ == "__main__":
    batch_layer()
