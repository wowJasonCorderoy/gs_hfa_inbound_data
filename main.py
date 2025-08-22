from google.cloud import storage
import pandas as pd
import re
import json
import re
from google.cloud import bigquery
from datetime import datetime,timezone  
import os
import hashlib
import numpy as np


def pretty_print_event(event: dict = None) -> None:
    """
    pretty print event dict.

    Args:
        event (dict, optional): [description]. Defaults to None.
    """
    print("event:")
    print(event)
    return

def pretty_print_context(context=None) -> None:
    """
    pretty print context dict.

    Args:
        event (dict, optional): [description]. Defaults to None.
    """
    print("context:")
    print(context)
    return

def get_file_name(event: dict = None) -> str:
    return event["name"]

def get_bucket_name(event: dict = None) -> str:
    return event["bucket"]

def save_to_bucket_name(bucketname: str) -> str:
    return bucketname + "_output"

def is_correctFileName(fileName: str = None, regex: str = r".*Inbound Report.*xls[x]?$") -> bool:
    """[summary]

    Args:
        fileName (str, optional): name of file. Defaults to None.
        regex (str, optional): regex expression to evaluate. Defaults to r".*Inbound Report.*xls[x]?$".

    Returns:
        bool: if True then regex matched fileName else False
    """
    import re

    if re.match(regex, fileName, re.IGNORECASE):
        return True
    else:
        return False

def gen_full_bucket_path(bucketName: str = None, fileName: str = None) -> str:
    return "gs://" + bucketName + "/" + fileName

def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def infer_site(filename: str):
    if 'trug' in filename.lower():
        return 'Truganina'
    elif 'heathwood' in filename.lower():
        return 'Heathwood'
    elif 'hw' in filename.lower():
        return 'Heathwood'
    elif 'bun' in filename.lower():
        return 'Bunbury'
    else:
        return 'Other'


def infer_species(filename: str):
    if 'beef' in filename.lower():
        return 'beef'
    elif 'lamb' in filename.lower():
        return 'lamb'
    elif 'pork' in filename.lower():
        return 'pork'
    else:
        return 'other'


def get_date(filename: str):
    import re
    import datetime
    extract_date = re.findall("([0-9]{1,2})\s([0-9]{1,2})\s+([0-9]{4})",filename)[0]
    extract_date_clean = extract_date[2]+'-'+extract_date[1]+'-'+extract_date[0]
    formatted_date = datetime.strptime(extract_date_clean, "%Y-%m-%d")#.strftime("%Y-%m-%d")
    return formatted_date


def load_hfa_inbound_data(file_path: str, da_date: datetime.date, site: str, species: str, sheet_name: str = "Service Kill"):
    headers = ['DESCRIPTION', 'ID', 'MANUFACTUREDDATE', 'PRODUCTID', 'STOCKINGPOINTID', 'SUPPLIERID', 'USERQUANTITY', 'SAPDELIVERYDATETIME', 'WOWSUPPLIER']
    dtypes = {'DESCRIPTION': 'str', 
              'ID': 'str', 
              'PRODUCTID': 'str',
              'STOCKINGPOINTID': 'str',
              'SUPPLIERID': 'str',
              'USERQUANTITY': np.float64,
              'WOWSUPPLIER': 'str'
            }
    parse_dates = ['MANUFACTUREDDATE', 'SAPDELIVERYDATETIME']
    try:
        df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:I", sheet_name=sheet_name)
    except:
        df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:I", sheet_name=0)
    df['filename_date'] = da_date
    df['filename_site'] = site
    df['filename_species'] = species
    return ("hfa_inbound2gen2",df)

def run(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'
    now_utc = datetime.now(timezone.utc) # timezone aware object, unlike datetime.utcnow().
    
    pretty_print_event(event)
    pretty_print_context(context)
    
    filename = get_file_name(event)
    print(f"Processing file: {filename}.")
    print(filename)
    
    # if not params file then abort
    if not is_correctFileName(filename):
        print(f"File {filename} is not correctly named. ABORTING.")
        return
    
    # if date not extractable from filename then don't error
    try:
        da_date = get_date(filename)
        da_date_string = da_date.strftime("%Y-%m-%d")
    except:
        da_date = datetime.strptime('1900-1-1', "%Y-%m-%d")
        da_date_string = ''
    
    
    site = infer_site(filename)
    species = infer_species(filename)
    
    bucketName = get_bucket_name(event)
    save_to_bucketname = save_to_bucket_name(bucketName)
    fileName_full = gen_full_bucket_path(bucketName, filename)
    
    # get params from bucket
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        params_blob = bucket.get_blob(filename)
        file_path = "/tmp/data_file.xlsx"
        params_blob.download_to_filename(file_path)
        print(f"{filename} saved to {file_path}")
    except:
        print(f"{filename} does not exist. ABORTING.")
        return
    
    saveFileName = now_utc.strftime("%Y%m%d_%H:%M:%S")+"_"+filename
    copy_blob(bucket_name=bucketName, blob_name=filename, destination_bucket_name=save_to_bucketname, destination_blob_name=saveFileName)
    
    # hash time
    file_contents_md5 = hashlib.md5(open(file_path,'rb').read()).hexdigest()
    
    func_2_run = [load_hfa_inbound_data]
    for func in func_2_run:
        tbl_name, df = func(file_path, da_date, site, species)
        df['upload_utc_dt'] = now_utc
        df['filename'] = filename
        df['file_contents_md5'] = file_contents_md5
        # save to BQ 
        bq_ds_tbl = f'hilton.{tbl_name}'
        print(f"writing {bq_ds_tbl}")
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, bq_ds_tbl, job_config=job_config)
        # save to cloud storage
        saveFileName = now_utc.strftime("%Y%m%d_%H:%M:%S")+"_"+site+"_"+species+"_"+tbl_name
        saveLocation = "gs://" + save_to_bucketname + "/" + saveFileName
        df.to_csv(saveLocation+'.csv', index=False)
        df.to_pickle(saveLocation+'.pk')

if __name__ == "__main__":
    run()
