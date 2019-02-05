import pandas as pd
import numpy as np
import sys
import argparse
import os
import tempfile
import shutil
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import CONFIG


def processArgs(args):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--meta',
        help='get the latest metadata from the Source To Target Mapping doc',
        action='store_true')
    args = parser.parse_args()

    # Set default options, then edit based on command line args
    options = {
        'LOAD_METADATA_FROM_GSHEET': False}

    if args.meta:
        options['LOAD_METADATA_FROM_GSHEET'] = True

    return options


def setup():

    # Check source data is there
    if not os.path.isfile(CONFIG['DATA_DIRECTORY'] + '/' +
                          CONFIG['INPUT_FILENAME']):
        raise ValueError('Failed to find the input data file. I expected to ' +
                         'find it in the same directory as the code, ' +
                         'named: ' + CONFIG['DATA_DIRECTORY'] + '/' +
                         CONFIG['INPUT_FILENAME'] + '. Either ' +
                         'add the file to the directory, or change the ' +
                         'expected file name in config.py')

    # Check Google API key is there
    if not os.path.isfile(CONFIG['GOOGLE_API_KEY_FILE']):
        raise ValueError('Failed to find the Google API key file. I ' +
                         'expected to find it in the same directory as the ' +
                         'code, named: ' + CONFIG['GOOGLE_API_KEY_FILE'] +
                         '. Either add the file to the directory, or ' +
                         'change the expected file name in config.py. ' +
                         'Read more about Google API Keys here: ' +
                         'https://developers.google.com/sheets/api/' +
                         'guides/authorizing')

    # Create/replace directory for custom field lists (this is the really
    # robust method fof deleting and creating a directory
    path = CONFIG['DATA_DIRECTORY'] + '/' + CONFIG['CUSTOM_FIELDS_DIRECTORY']
    if (os.path.exists(path)):
        tmp = tempfile.mktemp(
            dir=os.path.dirname(path))
        shutil.move(path, tmp)  # rename
        shutil.rmtree(tmp)  # delete
    os.makedirs(path)  # create the new folder


def loadMetadataFromGSheet():
    print('Connecting to Google Sheets')
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

    meta.to_csv(CONFIG['DATA_DIRECTORY'] + '/' + CONFIG['META_DATA_TMP_FILENAME'], index=False)

    return meta


def loadMetaDataFromTempFile():
    meta = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                       CONFIG['META_DATA_TMP_FILENAME'])
    return meta


def loadData():
    df = pd.read_csv(
        CONFIG['DATA_DIRECTORY'] + '/' + CONFIG['INPUT_FILENAME'],
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


def cleanData(df):

    # TODO: need to change col names back to legacy col names, because
    # col name mapping is now done last

    # Remove commas from ~12 last names
    df.loc[(df['last_name'].str.contains(',', na=False)) & (df['last_name'] != 'F. Queen, Jr.'), 'Last Name'] = df['last_name'].str.replace(',', '')

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


def outputMultiChoiceLists(df, meta):
    customFields = list(meta.loc[meta['Custom Field Type?'] == 'Multiple Choice', 'fullColName'])
    for col in customFields:
        customFieldValues = pd.DataFrame(df[col].unique()).dropna()
        customFieldValues.columns = ['VALUES']
        customFieldValues.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                                 CONFIG['CUSTOM_FIELDS_DIRECTORY'] + '/' +
                                 col + '.csv', index=False)


def processTags(df, meta):
    df_tagMapping = meta.loc[(meta['Tag?'] == 'T') & (meta['IN SCOPE'] == 'T'), ['fullColName', 'Tag Name']]
    tagMapping = df_tagMapping.set_index('fullColName')['Tag Name'].to_dict()

    # Add a tag column containing an empty list for every row, then loop
    # through the tagMapping. The tag mapping is a dictionary of column names
    # and tag values. The presence of a column name in the tag mapping
    # indicates that any row where this column is populated should be assigned
    # the tag. Note that multiple columns can be used for the same tag, so
    # we need to avoid creating duplicate tags
    df['tags'] = [[]] * len(df)
    for colName in tagMapping:
        for i, row in df.loc[df[colName].notna()].iterrows():
            # There's probably a more pythonic/vectorised way to do this, but
            # I had troube getting this working so am leaving it be. The key
            # was the .copy()
            list = row['tags'].copy()
            if tagMapping[colName] not in list:
                list.append(tagMapping[colName])
            df.at[i, 'tags'] = list

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


def outputData(df):

    df.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
              CONFIG['OUTPUT_FILENAME'], index=False)
    print("Saved data to " + CONFIG['OUTPUT_FILENAME'])


def run(args):

    opts = processArgs(args)

    setup()

    if opts['LOAD_METADATA_FROM_GSHEET']:
        meta = loadMetadataFromGSheet()
    else:
        meta = loadMetaDataFromTempFile()

    df = loadData()
    df = filterToInscopeColumns(df, meta)
    df = deleteTestData(df)

    outputMultiChoiceLists(df, meta)

    df = cleanData(df)

    df = processTags(df, meta)

    df = mapColumns(df, meta)

    outputData(df)


if __name__ == "__main__":
    run(sys.argv[1:])
