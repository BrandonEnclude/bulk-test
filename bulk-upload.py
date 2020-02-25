import csv
import codecs
import pandas as pd
import numpy as np
from creds import creds as creds
from datetime import datetime
import pytz
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from striprtf.striprtf import rtf_to_text

# Pass in Salesforce credentials here
bulk = SalesforceBulk(username=creds['username'], password=creds['password'], security_token=creds['token'], sandbox=True)

# Select SF Object to map to
job = bulk.create_insert_job("Correspondence__c", contentType='CSV', concurrency='Parallel')

# Read Excel spreadsheet to Pandas dataframe
df = pd.read_csv('Correspondence_Clean.csv', converters={'CorrespID': str, 'ContactID': str, 'ContactDate': str, 'CorrespType': str, 'Subject': str, 'Correspondence': str})

# Correspondence dictionary holder
correspondences = []

def formatDatetime(strDate):
    if not strDate or strDate == 'nan': return ''
    d = datetime.strptime(strDate[:19], "%Y-%m-%d %H:%M:%S")
    # print(f'{d.isoformat()}Z')
    return f'{d.isoformat()}Z'


# Iterate over rows in dataframe
for idx, row in enumerate(df.iterrows()):
    correspId = row[1]['CorrespID']
    contactId = row[1]['ContactID']
    contactDate = formatDatetime(row[1]['ContactDate'])
    correspondenceType = row[1]['CorrespType']
    subject = row[1]['Subject']
    correspondence = row[1]['Correspondence']
    # Create dictionary and add to correspondence holder
    correspondences.append(dict(Correspondence_Id__c=correspId, Contact_Id__c=contactId, Contact_Date__c=contactDate, Correspondence_Type__c = correspondenceType, Subject__c=subject, Body__c = correspondence))

# Create bulk job
csv_iter = CsvDictsAdapter(iter(correspondences))
batch = bulk.post_batch(job, csv_iter)
bulk.wait_for_batch(job, batch)
bulk.close_job(job)
print("Done. Correspondences uploaded.")

