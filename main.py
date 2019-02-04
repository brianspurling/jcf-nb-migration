import pandas as pd
import numpy as np
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import CONFIG


def loadMetaDataFromGSheet():
    _client = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(
            CONFIG['GOOGLE_API_KEY_FILE'],
            CONFIG['GOOGLE_API_SCOPE']))
    conn = _client.open(CONFIG['META_DATA_GSHEET_NAME'])

    stm = None
    for ws in conn.worksheets():
        if ws.title == 'STM':
            stm = ws
            break

    meta = pd.read_json(json.dumps(stm.get_all_records()))

    meta.to_csv(CONFIG['META_DATA_TMP_FILENAME'], index=False)

    return meta


def loadMetaDataFromTempFile():
    meta = pd.read_csv(CONFIG['META_DATA_TMP_FILENAME'])
    return meta


def loadData():
    df = pd.read_csv(
        CONFIG['INPUT_FILENAME'],
        low_memory=False,
        dtype={'Work Phone': 'object'})

    expectedSize = (CONFIG['EXPECTED_ROW_COUNT'], CONFIG['EXPECTED_COL_COUNT'])
    if df.shape != expectedSize:
        raise ValueError("Size of dataset has changed! Expecting " +
                         str(expectedSize) + ", got " + str(df.shape))
    return df


def filterToInscopeColumns(df, meta):
    # Some of the column names have carriage returns in, which
    # is a problem for matching to our list of in-scope columns.
    allCols = df.columns.str.replace('\n', '')
    df.columns = allCols

    # Our meta data has a "IN SCOPE" column, T or F
    inScopeCols = meta.loc[meta['IN SCOPE'] == 'T', 'fullColName']
    df = df[list(inScopeCols)]

    return df


def deleteTestData(df):

    df_testRows = df[
        df.apply(
            lambda row: row.astype(str).str.contains('test', case=False).any(),
            axis=1) &
        (df['Parliamentary Constituency (U.K.)'] != 'Southampton, Test')]

    df_merged = pd.merge(df, df_testRows, indicator=True, how='outer')
    df = df_merged.query('_merge=="left_only"').drop('_merge', axis=1)

    print("Deleted " + str(df_testRows.shape[0]) +
          " rows. See deleted_test_rows.csv")
    df_testRows.to_csv('deleted_test_rows.csv', index=False)

    return df


def mapColumns(df, meta):

    # Get a dictionary of our two meta data columns (orig name, NB name)

    mapping = {}
    targetColList = []
    for i, row in meta.iterrows():
        if row['IN SCOPE'] == 'T':
            mappedValue = row['NB TARGET FIELD']
            if row['NB TARGET FIELD'] is np.nan:
                mappedValue = 'NOT MAPPED - ' + str(i)

            if mappedValue in targetColList:
                mapping[row['fullColName']] = mappedValue + ' - ' + str(i)
            else:
                mapping[row['fullColName']] = mappedValue

            targetColList.append(mappedValue)

    df.rename(
        index=str,
        columns=mapping,
        inplace=True)

    return df


def cleanData(df):

    # Remove commas from ~12 last names
    df.loc[(df['last_name'].str.contains(',', na=False)) & (df['last_name'] != 'F. Queen, Jr.'),'Last Name'] = df['last_name'].str.replace(',', '')

    # Delete address fields that are just commas
    df.loc[(df['address1'] == ', '), 'address1'] = np.nan
    df.loc[(df['address1'] == ','), 'address1'] = np.nan

    # Lower case some city names
    df.loc[df['city'].str.match('^.*[A-Z]$', na=False), 'city'] = df['city'].str.title()

    # Manually fix some city names
    df.loc[df['city'] == 'St. Mary&#039;s Ward', 'city'] = "St. Mary's Ward"

    # Replace seven "0" phone numbers with nan
    # TODO: change this to regex, there are 0000 etc too
    # df.loc[df['Home Phone'] == '0', 'Home Phone'] = np.nan

    return df




def outputData(df):

    df.to_csv(CONFIG['OUTPUT_FILENAME'], index=False)
    print("Saved data to " + CONFIG['OUTPUT_FILENAME'])


def run():

    # meta = loadMetaDataFromGSheet()
    meta = loadMetaDataFromTempFile()

    df = loadData()
    df = filterToInscopeColumns(df, meta)
    df = deleteTestData(df)

    df = mapColumns(df, meta)

    df = mapColumns(df)

    outputData(df)


run()
