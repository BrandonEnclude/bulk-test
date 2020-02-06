import csv
import codecs
import pandas as pd
from creds import creds as creds
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from striprtf.striprtf import rtf_to_text

# Pass in Salesforce credentials here
bulk = SalesforceBulk(username=creds['username'], password=creds['password'], security_token=creds['token'])

# Select SF Object to map to
job = bulk.create_insert_job("Correspondence__c", contentType='CSV', concurrency='Parallel')

# Read Excel spreadsheet to Pandas dataframe
df = pd.read_excel('Correspondence_Table.xlsx', sheet_name='Sheet1')

# Correspondence dictionary holder
cors = []

# Iterate over rows in dataframe
for idx, row in enumerate(df.iterrows()):
    cell = row[1]['Correspondence'] #Select Correspondence cell
    if isinstance(cell, str): #Check if cell value is a string (If it's empty, Pandas interprets it as NaN)
        cell = cell[2:].strip('FFFFFFFF') #Strip off 'x0' prefix and trailing whitespace (Sometimes an ungodly ammount)
        try:
            #Create dict for SF object
            #(Optional) Strip RTF tags
            #Decode from hex to utf-8 (or ASCII ?)
            cors.append(dict(Body__c=rtf_to_text(bytes.fromhex(cell).decode('utf-8', 'backslashignore'))))
        except Exception:
            #There are more exceptions that need to be handled
            pass
    else:
        cors.append(dict(Body__c=' ')) #Cell is NaN (Blank), write a blank for now, but maybe throw away

csv_iter = CsvDictsAdapter(iter(cors))

batch = bulk.post_batch(job, csv_iter)

bulk.wait_for_batch(job, batch)
bulk.close_job(job)
print("Done. Accounts uploaded.")

