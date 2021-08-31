try:
    from google.cloud import storage
    import google.cloud.storage
    import json
    import os
    import sys
except Exception as e:
    print("Error : {} ".format(e))

#setting environment path
PATH = os.path.join(os.getcwd() , 'gcp-assignment-322710-3df4fa1ed557.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
storage_client = storage.Client(PATH)
print(storage_client)

#Getting all Files from GCP Bucket
bucket = storage_client.get_bucket('sv-bucket')
filename = [filename.name for filename in list(bucket.list_blobs(prefix='')) ]
print(filename)

#Pushing a file on GCP bucket
def upload_csv(file_name,bucket_name):
    upload_file = os.path.join(os.getcwd(), file_name)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(upload_file)

upload_csv('Orders.csv','sv-bucket')
upload_csv('Customers.csv','sv-bucket')


