import pandas as pd
import numpy as np
import sys
import argparse
import os
import tempfile
import shutil
import json
import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials

from config import CONFIG


def processArgs(args):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--setup',
        help="Only run the setup process - doesn't load any data",
        action='store_true')
    parser.add_argument(
        '--meta',
        help='Get the latest metadata from the Source To Target Mapping doc',
        action='store_true')
    args = parser.parse_args()

    # Set default options, then edit based on command line args
    options = {
        'LOAD_METADATA_FROM_GSHEET': False,
        'ONLY_RUN_SETUP': False}

    if args.meta:
        options['LOAD_METADATA_FROM_GSHEET'] = True
    if args.setup:
        options['ONLY_RUN_SETUP'] = True

    return options


def setup():

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
    if os.path.exists(path):
        tmp = tempfile.mktemp(
            dir=os.path.dirname(path))
        shutil.move(path, tmp)  # rename
        shutil.rmtree(tmp)  # delete
    os.makedirs(path)  # create the new folder

    # Check whether the meta data text file exists and, if it doesn't,
    # warn that it needs to be created
    path = CONFIG['DATA_DIRECTORY'] + '/' + CONFIG['META_DATA_TMP_FILENAME']
    if not os.path.isfile(path):
        print('Did not find a meta data text file (' + path + '). The first ' +
              'time you run the pipeline use the `--meta` argument to pull ' +
              'the latest meta data from the Google Sheet')


def loadMetadataFromGSheet():

    # Meta data and cleaned religion data is in the same spreadsheet

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
        if ws.title == 'RELIGIONS':
            religions = ws

    meta = pd.read_json(json.dumps(stm.get_all_records()))
    rels = pd.read_json(json.dumps(religions.get_all_records()))

    meta.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                CONFIG['META_DATA_TMP_FILENAME'], index=False)

    rels.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                CONFIG['RELIGIONS_MAP_TMP_FILENAME'], index=False)

    # Cleaned repeated-values data is in a different spreadsheet
    _client = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(
            CONFIG['GOOGLE_API_KEY_FILE'],
            CONFIG['GOOGLE_API_SCOPE']))
    conn = _client.open(CONFIG['REPEATED_DATA_GSHEET_NAME'])

    repData = {}
    for col in CONFIG['COLS_WITH_REPEATD_DATA']:
        ws = conn.worksheet(col[0:99])
        repData[col] = pd.read_json(json.dumps(ws.get_all_records()))
        filename = col[0:99].replace('/', '')
        repData[col].to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                            'repData_' + filename + '.csv',
                            index=False)

    return (meta, rels, repData)


def loadMetaDataFromTempFile():
    meta = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                       CONFIG['META_DATA_TMP_FILENAME'])
    rels = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                       CONFIG['RELIGIONS_MAP_TMP_FILENAME'])
    repData = {}
    for col in CONFIG['COLS_WITH_REPEATD_DATA']:
        filename = col[0:99].replace('/', '')
        repData[col] = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                                   'repData_' + filename + '.csv')
    return (meta, rels, repData)


def loadData(meta):

    # Check source data is there
    if not os.path.isfile(CONFIG['DATA_DIRECTORY'] + '/' +
                          CONFIG['INPUT_FILENAME']):
        raise ValueError('Failed to find the input data file. I expected to ' +
                         'find it in the same directory as the code, ' +
                         'named: ' + CONFIG['DATA_DIRECTORY'] + '/' +
                         CONFIG['INPUT_FILENAME'] + '. Either ' +
                         'add the file to the directory, or change the ' +
                         'expected file name in config.py')

    df = pd.read_csv(
        CONFIG['DATA_DIRECTORY'] + '/' + CONFIG['INPUT_FILENAME'],
        low_memory=False,
        dtype={'Work Phone': 'object'})

    # Some of the column names have carriage returns in, which
    # is a problem for matching to our list of in-scope columns.
    allCols = df.columns.str.replace('\n', '')
    df.columns = allCols

    expectedSize = (CONFIG['EXPECTED_ROW_COUNT'], CONFIG['EXPECTED_COL_COUNT'])
    if df.shape != expectedSize:
        print("ERROR: Size of dataset has changed! Expecting " +
              str(expectedSize) + ", got " + str(df.shape) + '. Either fix ' +
              'the import file or change the expected value in config.py')
        sys.exit()

    # Make sure we have meta data for every imported column

    if (len(list(set(list(df)) - set(meta['fullColName'].tolist()))) > 0 or
            len(list(set(meta['fullColName'].tolist()) - set(list(df)))) > 0):

        print()
        print('WARNING: columns in imported data do not match columns in ' +
              'meta data')
        print()
        print('*** outputting imported data columns not in meta data ' +
              'columns to file dataColsMissingFromMeta.csv ***')
        print()
        cols = list(set(list(df)) - set(meta['fullColName'].tolist()))
        with open('dataColsMissingFromMeta.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for col in cols:
                wr.writerow([col, ])
        print('*** outputting meta data columns not in imported data ' +
              'columns to file metaColsMissingFromData.csv ***')
        print()
        cols = list(set(meta['fullColName'].tolist()) - set(list(df)))
        with open('metaColsMissingFromData.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for col in cols:
                wr.writerow([col, ])
        print()

    return df

# A temporary function to check a bunch of non-meta dataed columsn were empty
# def checkEmptyColsAreEmpty(df, meta):
#     allOK = True
#     for colName in meta.loc[meta['IN SCOPE'] == '?', 'fullColName'].tolist():
#         nonNullVals = df.loc[df[colName].notnull(), colName].tolist()
#         if len(nonNullVals) > 0:
#             allOK = False
#             print("|" + colName + "| >> meta data is not populated, but " +
#                   "the column " +
#                   "in the data export is not empty. It contains: ")
#             print()
#             print('------')
#             print(nonNullVals)
#             print('------')
#             print()
#     if allOK:
#         print('All ok')


def filterToInscopeColumns(df, meta):

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


def outputColumnsWithRepeatedData(df):

    _client = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(
            CONFIG['GOOGLE_API_KEY_FILE'],
            CONFIG['GOOGLE_API_SCOPE']))
    ss = _client.open(CONFIG['REPEATED_DATA_GSHEET_NAME'])

    cols = CONFIG['COLS_WITH_REPEATED_DATE']

    existingWSs = ss.worksheets()
    existingWSTitles = []
    for ws in existingWSs:
        existingWSTitles.append(ws.title)

    for col in cols:

        if col[0:99] in existingWSTitles:
            ss.del_worksheet(ss.worksheet(col[0:99]))

        ws = ss.add_worksheet(title=col[0:99],
                              rows=len(df.loc[df[col].notna()])+1,
                              cols=3)

        # We build up our new GSheets table first, in memory,
        # then write it all in one go.
        cell_list = ws.range('A1:C'+str(len(df.loc[df[col].notna()])+1))

        rangeRowCount = 0
        cell_list[rangeRowCount].value = 'Email'
        cell_list[rangeRowCount+1].value = col
        cell_list[rangeRowCount+2].value = 'Length'

        rangeRowCount += 3

        df_op = df.loc[df[col].notna(), ['Email', col]]
        df_op['length'] = df_op[col].str.len()
        df_op = df_op.sort_values('length', ascending=False)
        for i, row in df_op.iterrows():
            cell_list[rangeRowCount].value = row['Email']
            cell_list[rangeRowCount+1].value = row[col]
            cell_list[rangeRowCount+2].value = row['length']
            rangeRowCount += 3

        ws.update_cells(cell_list)


def outputReligionData(df):
    rels = pd.DataFrame(df['Are you a person of faith?'].unique()).dropna()
    rels.columns = ['VALUES']
    rels.to_csv('data/relgions_for_cleaning.csv', index=False)


def cleanData(df, rels, repData):

    # Remove commas from ~12 last names
    df.loc[(df['Last Name'].str.contains(',', na=False)) &
           (df['Last Name'] != 'F. Queen, Jr.'),
           'Last Name'] = df['Last Name'].str.replace(',', '')

    # Delete address fields that are just commas
    df.loc[(df['Address 1'] == ', '), 'Address 1'] = np.nan
    df.loc[(df['Address 1'] == ','), 'Address 1'] = np.nan

    # Lower case some city names
    df.loc[df['City'].str.match('^.*[A-Z]$', na=False), 'City'] = \
        df['City'].str.title()

    # Manually fix some city names
    df.loc[df['City'] == 'St. Mary&#039;s Ward', 'City'] = "St. Mary's Ward"

    # Remove "0" zip codes
    df.loc[df['Zip'] == '0', 'Zip'] = np.nan

    # Fix the typo email address
    df.loc[df['Email'] == 'a..murdock@dsl.pipex.com', 'Email'] = \
        'a.murdock@dsl.pipex.com'

    # Remove invalid phone numbers with nan
    df.loc[df['Home Phone'].isin([
           '0', '999', '01', '07', '34', '84', '447511', '447911']),
           'Home Phone'] = np.nan

    # Delete the parliament number, that is on 28 records
    df.loc[df['Work Phone'] == '02072193000', 'Work Phone'] = np.nan

    # Change date format
    df['Join Date - year'] = df['Join Date'].str.slice(0, 4)
    df['Join Date - month'] = df['Join Date'].str.slice(5, 7)
    df['Join Date - day'] = df['Join Date'].str.slice(8, 10)

    df['Join Date'] = df['Join Date - month'].astype(str) + '/' + \
        df['Join Date - day'].astype(str) + '/' + \
        df['Join Date - year'].astype(str)

    df.drop(
        ['Join Date - year', 'Join Date - month', 'Join Date - day'],
        axis=1,
        inplace=True)

    # Clean religion columns based on mapping
    new_df = pd.merge(
        df,
        rels,
        how='left',
        left_on=['Are you a person of faith?'],
        right_on=['Values in Data'])

    df['Are you a person of faith?'] = new_df['Replacement Values']

    # Clean columns that have repeated data (these have been manually
    # cleaned in Google Sheets, and just need reading back in and the original
    # values overriting
    for col in CONFIG['COLS_WITH_REPEATD_DATA']:
        cleanedData = repData[col]
        new_df = pd.merge(
            df,
            cleanedData,
            how='left',
            left_on=['Email'],
            right_on=['Email'])
        df[col] = new_df[col + '_y']

    # Remove country and /t from the columns: Girlguiding Sign Up:County
    # Girlguiding Sign Up:County

    # Replace newline char with ','
    colName = ("Girlguiding Sign Up:If you'd like us to post you an ideas " +
               "pack, please fill out your address details:")
    df[colName] = df[colName].str.replace('\n', ', ')

    colName = ("Scouts Events:If you'd like us to post you an ideas pack, " +
               "please fill out your address details:")
    df[colName] = df[colName].str.replace('\n', ', ')

    # Replace strings "Na: and "None" with empty string
    colName = 'Organisational/company sign up:Name of Organisation'
    df.loc[df[colName].isin(["None", "Na"]), colName] = ''

    return df


def outputMultiChoiceLists(df, meta):
    customFields = list(meta.loc[
        meta['Custom Field Type?'] == 'Multiple Choice', 'fullColName'])

    for col in customFields:
        customFieldValues = pd.DataFrame(df[col].unique()).dropna()
        customFieldValues.columns = ['VALUES']
        customFieldValues.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                                 CONFIG['CUSTOM_FIELDS_DIRECTORY'] + '/' +
                                 col + '.csv', index=False)


def processTags(df, meta):
    df_tagMapping = meta.loc[(meta['Tag?'] == 'T') & (meta['IN SCOPE'] == 'T'),
                             ['fullColName', 'Tag Name']]
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

    df['tags'] = df['tags'].apply(lambda x: ','.join(map(str, x)))

    return df


def mapColumns(df, meta):

    print()

    mapping = {}
    reverseMapping = {}
    colsToDrop = []
    for i, row in meta.iterrows():
        if row['IN SCOPE'] == 'T':
            mappedValue = row['NB TARGET FIELD']
            if row['NB TARGET FIELD'] is np.nan:
                mappedValue = 'NOT MAPPED - ' + str(i)

            # Some source columns have been mapped to the same target columns.
            # These columns need merging
            if mappedValue in reverseMapping:

                existingColName = reverseMapping[mappedValue]
                newColName = row['fullColName']
                print('Merging columns >>> ' +
                      '\n  - ' + existingColName +
                      '\n  - ' + newColName +
                      '\nInto >>> ' +
                      '\n  - ' + mappedValue)
                df[existingColName] = df[existingColName].fillna('')
                df[newColName] = df[newColName].fillna('')

                examplePrinted = False

                for j, dfrow in df.iterrows():

                    doMerge = True
                    # If the the two values are the same, don't merge
                    if dfrow[existingColName] == dfrow[newColName]:
                        doMerge = False
                    # If one of the the two values are blank, don't merge
                    elif (dfrow[existingColName] == '' or
                            dfrow[newColName] == ''):
                        doMerge = False

                    if doMerge:
                        dfrow[existingColName] = \
                            str(dfrow[existingColName]) + \
                            '  |  ' + \
                            str(dfrow[newColName])
                        if not examplePrinted:
                            print('** Example of merged value: ' +
                                  str(dfrow[existingColName]) + '\n')
                            examplePrinted = True
                            print(dfrow['Email'])

                        colsToDrop.append(newColName)
                if not examplePrinted:
                    print('** No merging needed **\n')

            else:
                mapping[row['fullColName']] = mappedValue

            reverseMapping[mappedValue] = row['fullColName']

    # Drop the columns we merged together
    df.drop(
        colsToDrop,
        axis=1,
        inplace=True)

    # And rename everything

    df.rename(
        index=str,
        columns=mapping,
        inplace=True)

    return df


def outputData(df):

    df.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
              CONFIG['OUTPUT_FILENAME'], index=False)
    df.head(10000).to_csv(CONFIG['DATA_DIRECTORY'] + '/' + 'subset_' +
                          CONFIG['OUTPUT_FILENAME'], index=False)
    print("Saved data to " + CONFIG['OUTPUT_FILENAME'])


def run(args):

    opts = processArgs(args)

    setup()

    if opts['ONLY_RUN_SETUP']:
        sys.exit()

    if opts['LOAD_METADATA_FROM_GSHEET']:
        (meta, rels, repData) = loadMetadataFromGSheet()
    else:
        (meta, rels, repData) = loadMetaDataFromTempFile()

    df = loadData(meta)

    df = filterToInscopeColumns(df, meta)

    # df = deleteTestData(df)

    # If you uncomment this, it will overwrite the repeated-values spreadsheet,
    # which you probably don't want to do, given that JCF have already manually
    # cleaned the data in this spreadsheet!
    # outputColumnsWithRepeatedData(df)

    # outputReligionData(df)

    df = cleanData(df, rels, repData)

    outputMultiChoiceLists(df, meta)

    df = processTags(df, meta)

    # df = mapColumns(df, meta)

    outputData(df)


if __name__ == "__main__":
    run(sys.argv[1:])
