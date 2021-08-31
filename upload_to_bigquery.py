try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import os
    import io
    from io import BytesIO
    from google.cloud import storage
    import pandas as pd
    from pandas.io import gbq
    from pandas import Series, DataFrame
    from pandas.core.generic import NDFrame
    from pandas.io.parsers import TextFileReader
except Exception as e:
    print("Error : {} ".format(e))

#setting environment path
PATH = os.path.join(os.getcwd() , 'gcp-assignment-322710-3df4fa1ed557.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
storage_client = storage.Client(PATH)
# print(storage_client)

#Getting all Files from GCP Bucket
bucket = storage_client.get_bucket('sv-bucket')
filename = [filename.name for filename in list(bucket.list_blobs(prefix='')) ]
# print(filename)

#Reading CSV file directly from GCP bucket
df1 = pd.read_csv(io.BytesIO(bucket.blob(blob_name = 'Customers.csv').download_as_string()) ,encoding='UTF-8',sep=',')
df2 = pd.read_csv(io.BytesIO(bucket.blob(blob_name = 'Orders.csv').download_as_string()) ,encoding='UTF-8',sep=',')
# print(df1)
# print(df2)

#join files using customerID key
mergeCsv = pd.merge(df1, df2, how='left', on='CustomerID')
mergeCsv.to_csv("result.csv", index=False)
# print(mergeCsv)

#download files
def download_file(blob_name,file_name):
    try:
        file_path = os.path.join(os.getcwd(),file_name)
        blob = bucket.blob(blob_name)
        with open(file_path, 'wb') as f:
            storage_client.download_blob_to_file(blob, f)

    except Exception as err:
        print(err.args)
        raise Exception("Downloading file Failed")


download_file("Customers.csv",'download_customers.csv')
download_file("Orders.csv",'download_orders.csv')

def create_dataset(project_id = "gcp-assignment-322710"):
    try:
        dataset_id = "{}.sv_dataset".format(project_id)
        dataset = bigquery.Dataset(dataset_id)
        dataset = storage_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(storage_client.project, dataset.dataset_id))

    except Exception as err:

        print(err.args)
        raise Exception("Dataset creation failed")
# create_dataset()
def create_table(project_id, dataset_id, table_name,schema):
    try:
        table_id = "{}.{}.{}".format(project_id, dataset_id, table_name)
        table = bigquery.Table(table_id, schema=schema)
        table = storage_client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    except Exception as err:

        print(err.args)
        raise Exception("Table creation failed")

schema = [
            bigquery.SchemaField("CustomerID", "INTEGER"),
            bigquery.SchemaField("CustomerName", "STRING"),
            bigquery.SchemaField("ContactName", "STRING"),
            bigquery.SchemaField("Address", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("PostalCode", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("OrderID", "FLOAT"),
            bigquery.SchemaField("EmployeeID", "FLOAT"),
            bigquery.SchemaField("OrderDate", "DATE"),
            bigquery.SchemaField("ShipperID", "FLOAT"),
        ]
# create_table("gcp-assignment-322710","sv_dataset","sv_table1",schema)
def load_data():
    try:
        res = pd.read_csv("result.csv")
        res.to_gbq(destination_table='sv_dataset.sv_table',project_id='gcp-assignment-322710',if_exists='fail')
        print("Data loaded successfully")
    except Exception as err:

        print(err.args)
        raise Exception("Loading Data failed")

load_data()