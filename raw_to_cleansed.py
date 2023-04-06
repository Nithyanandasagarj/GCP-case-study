#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import io 
import datetime
import csv
from google.cloud import storage
from google.cloud.storage import Blob

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #file = event
    file_name = event['name']
    bucket_name= event['bucket']
    storage_client=storage.Client()
    bucket=storage_client.get_bucket(bucket_name)
    blob=bucket.blob(file_name)
    # blobs = list(source_bucket.list_blobs(prefix=''))
    file_content=blob.download_as_string()

    csv_reader = csv.reader(io.StringIO(file_content.decode('utf-8')))
    modified_csv_rows = []
    count=0
    for i, row in enumerate(csv_reader):
        if '' in row:
            i=i+1
        else:
            if i==0:
                row.append("timestamp")
                modified_csv_rows.append(row)
            else:
                row[0]=str(count)
                count=int(count)+1
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                row.append(timestamp)
                modified_csv_rows.append(row)

    new_file_name = "cleaned_" + file_name
    destination_bucket=storage_client.get_bucket("sagar_cleansed_bucket")
    new_blob = destination_bucket.blob(new_file_name)
    new_blob.upload_from_string('\n'.join([','.join(row) for row in modified_csv_rows]))

    print(f"Added new column with current timestamp to {file_name} and saved as {new_file_name} in bucket {bucket_name}")



    #print(f"Processing file: {file['name']}.")

