import os
import re
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from datetime import datetime,timezone
import hashlib
import openpyxl

#### Declare functions
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

def get_date(filename: str):
    import re
    import datetime
    extract_date = re.findall("([0-9]{1,2})\s([0-9]{1,2})\s+([0-9]{4})",filename)[0]
    extract_date_clean = extract_date[2]+'-'+extract_date[1]+'-'+extract_date[0]
    formatted_date = datetime.datetime.strptime(extract_date_clean, "%Y-%m-%d")#.strftime("%Y-%m-%d")
    return formatted_date

def load_hfa_inbound_data(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Sheet1"):
    headers = ['DESCRIPTION', 'ID', 'MANUFACTUREDDATE', 'PRODUCTID', 'STOCKINGPOINTID', 'SUPPLIERID', 'USERQUANTITY', 'SAPDELIVERYDATETIME']
    dtypes = {'DESCRIPTION': 'str', 
              'ID': 'str', 
              'PRODUCTID': 'str',
              'STOCKINGPOINTID': 'str',
              'SUPPLIERID': 'str',
              'USERQUANTITY': np.float64,
            }
    parse_dates = ['MANUFACTUREDDATE', 'SAPDELIVERYDATETIME']
    try:
        df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:H", sheet_name=sheet_name)
    except:
        df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:H", sheet_name=0)
    df['filename_date'] = da_date
    df['filename_site'] = site
    return ("hfa_inbound",df)

### Save to bigquery
def get_bq_credentials():
    try:
        client = service_account.Credentials.from_service_account_file(**{
        'filename':CREDS_FILE_LOC, 
        'scopes':["https://www.googleapis.com/auth/cloud-platform"],
        })
    except:
        client = bigquery.Client(project=PROJECT_ID)
    return client
    
#### Declare constants
PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'

project_creds_file_map = {
    'gcp-wow-pvc-grnstck-prod':r"C:\dev\greenstock\optimiser_files\key_prod.json",
    'gcp-wow-pvc-grnstck-dev':r"C:\dev\greenstock\optimiser_files\key_dev.json",
}
CREDS_FILE_LOC = project_creds_file_map.get(PROJECT_ID)

now_utc = datetime.now(timezone.utc)

filename = "HFA Inbound Report 07 04 2022.xlsx"
file_path = os.path.abspath(filename)
da_date = get_date(filename)
da_date_string = da_date.strftime("%Y-%m-%d")
site = infer_site(filename)

#### run time
def run_local():
    if not is_correctFileName(filename):
            print(f"File {filename} is not correctly named. ABORTING.")
            return

    file_contents_md5 = hashlib.md5(open(filename,'rb').read()).hexdigest()
    
    func_2_run = [load_hfa_inbound_data]
    for func in func_2_run:
        tbl_name, df = func(file_path, da_date, site)
        df['upload_utc_dt'] = now_utc
        df['filename'] = filename
        df['file_contents_md5'] = file_contents_md5
        # save to BQ 
        credentials = get_bq_credentials()
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = PROJECT_ID
        bq_ds_tbl = f'hilton.{tbl_name}'
        print(f"writing {bq_ds_tbl}")
        pd.io.gbq.to_gbq(df, bq_ds_tbl, PROJECT_ID, chunksize=100000, reauth=False, if_exists='append')

if __name__ == "__main__":
    run_local()
