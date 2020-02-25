import csv
import codecs
import pandas as pd
from creds import creds as creds
import progressbar
from striprtf.striprtf import rtf_to_text
import re
import sys

# Truncate errors.csv before run
errors = open('errors.csv', 'a')
errors.truncate(0)

# Read CSV and convert to dataframe
print('Reading CSV as dataframe...')
df = pd.read_csv('Correspondence.csv')
### If memory error, comment out above line and read CSV in chunks instead: ###
# chunkList = []
# for chunk in  pd.read_csv('Correspondence.csv', chunksize=20000):
#     chunkList.append(chunk)
# df = pd.concat(chunkList, axis= 0)
# del chunkList

numSuccess = 0
numError = 0
numEmpty = 0
totalRows = df.shape[0]
print(f'Number of rows: {totalRows}')

def decode(cell):
    # Progress Calculation
    global numSuccess
    global numError
    global totalRows
    global numEmpty
    sys.stdout.write(f'Successful: {numSuccess}; Errors: {numError}; Empty: {numEmpty}; Percent Done: { round((numSuccess + numError + numEmpty) / totalRows, 4) * 100 }% \r')
    sys.stdout.flush()

    # Return empty string for empty-ish values
    if not isinstance(cell, str) or cell == '0x00' or cell is None or pd.isnull(cell):
        numEmpty += 1
        return ''

    try:
        html_reg = re.compile('<.*?>') # Regex to match HTML tags
        cell = cell[2:] if cell[:2] == '0x' else cell #Remove 0x prefix
        cell = bytes.fromhex(cell).decode('latin1', errors='replace') #Decode from hex
        try:
            cell = rtf_to_text(cell)  # Strip RTF tags
        except TypeError as e: #Some 'NoneType' values still slipping through; counting as empty not errors
            errors.write(f'{numEmpty + numError + numSuccess},{str(e).replace(",", " ")}\n')
            numEmpty += 1
            return ''
        cell = re.sub(html_reg, '', cell) # Strip HTML tags
        cell = cell.strip() # Remove leading and trailing whitespace
        numSuccess += 1
        return cell
    except Exception as e:
        # Write errors to errors.csv
        numError += 1
        errors.write(f'{numEmpty + numError + numSuccess},{str(e).replace(",", " ")}\n')
        print(e)
        return ''

print('Decoding...')
df['Correspondence'] = df['Correspondence'].apply(lambda cell: decode(cell) )

# Close errors; Write successful to Correspondence_Clean.csv
errors.close()
df.to_csv('Correspondence_Clean.csv', sep=',')

print('Done.')
print(f'Successful: {numSuccess}; Errors: {numError}; Empty: {numEmpty}')