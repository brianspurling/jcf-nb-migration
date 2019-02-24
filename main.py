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


def logFunctionStart(funcDesc):

    spacer = ' ' * (62 - len(funcDesc))

    op = ''
    op += '\n'
    op += '**************************************************************'
    op += '***\n'
    op += '*                                                             '
    op += '  *\n'
    op += '* ' + funcDesc + spacer + '*\n'
    op += '*                                                             '
    op += '  *\n'
    op += '**************************************************************'
    op += '***\n'

    print(op)


def logFunctionEnd(report=None):

    op = ''

    if report is not None and report != '':
        op += report + '\n\n'

    op += '[Completed]'

    print(op)


def setup():

    funcName = 'Setting Up Pipeline'
    logFunctionStart(funcName)

    report = ''

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
        report += ('Did not find a meta data text file (' + path + '). ' +
                   'The first time you run the pipeline use the `--meta` ' +
                   'argument to pull the latest meta data from the Google ' +
                   'Sheet')

    logFunctionEnd(report)


def loadMetadataFromGSheet():

    funcName = 'Loading Meta Data from Google Sheet'
    logFunctionStart(funcName)

    # Meta data and cleaned religion data is in the same spreadsheet

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

    logFunctionEnd()

    return (meta, rels, repData)


def loadMetaDataFromTempFile():

    funcName = 'Loading Meta Data from CSV'
    logFunctionStart(funcName)

    meta = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                       CONFIG['META_DATA_TMP_FILENAME'])
    rels = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                       CONFIG['RELIGIONS_MAP_TMP_FILENAME'])
    repData = {}
    for col in CONFIG['COLS_WITH_REPEATD_DATA']:
        filename = col[0:99].replace('/', '')
        repData[col] = pd.read_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                                   'repData_' + filename + '.csv')

    logFunctionEnd()

    return (meta, rels, repData)


def loadData(meta):

    funcName = 'Loading Data from CSV'
    logFunctionStart(funcName)
    report = ''

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

    # For testing
    # df = df.loc[df['Email'].isin([])]

    expectedSize = (CONFIG['EXPECTED_ROW_COUNT'], CONFIG['EXPECTED_COL_COUNT'])
    if df.shape != expectedSize:
        raise ValueError("ERROR: Size of dataset has changed! Expecting " +
                         str(expectedSize) + ", got " + str(df.shape) + '. ' +
                         'Either fix the import file or change the expected ' +
                         'value in config.py')
        sys.exit()
    else:
        report += ('Loaded ' + str(df.shape[0]) + ' rows and ' +
                   str(df.shape[1]) + ' columns')

    # Make sure we have meta data for every imported column

    if (len(list(set(list(df)) - set(meta['fullColName'].tolist()))) > 0 or
            len(list(set(meta['fullColName'].tolist()) - set(list(df)))) > 0):

        cols = list(set(list(df)) - set(meta['fullColName'].tolist()))

        report += ('WARNING: columns in imported data do not match columns ' +
                   'in meta data\n\n')
        report += (' - Outputting imported data columns not in meta data ' +
                   'to file dataColsMissingFromMeta.csv ***\n')
        report += (' - Outputting meta data columns not in imported dataset ' +
                   'to file metaColsMissingFromData.csv ***\n')

        with open('dataColsMissingFromMeta.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for col in cols:
                wr.writerow([col, ])

        cols = list(set(meta['fullColName'].tolist()) - set(list(df)))
        with open('metaColsMissingFromData.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for col in cols:
                wr.writerow([col, ])

    logFunctionEnd(report)

    return df


def filterToInscopeColumns(df, meta):

    funcName = 'Filtering to In Scope Columns'
    logFunctionStart(funcName)
    report = ''

    report += 'Reduced dataset from ' + str(df.shape[1]) + ' columns to '

    # Our meta data has a "IN SCOPE" column, T or F
    inScopeCols = meta.loc[meta['IN SCOPE'] == 'T', 'fullColName']
    df = df[list(inScopeCols)]

    report += str(df.shape[1]) + ' columns (where IN SCOPE column of STM is T)'

    logFunctionEnd(report)

    return df


def deleteTestData(df):

    funcName = 'Deleting Test Data'
    logFunctionStart(funcName)
    report = ''

    print('This will take a while...\n')

    df_testRows = df[
        df.apply(
            lambda row: row.astype(str).str.contains('test', case=False).any(),
            axis=1) &
        (df['Parliamentary Constituency (U.K.)'] != 'Southampton, Test')]

    df_merged = pd.merge(df, df_testRows, indicator=True, how='outer')
    df = df_merged.query('_merge=="left_only"').drop('_merge', axis=1)

    report += ('Deleted ' + str(df_testRows.shape[0]) + ' rows. See ' +
               'deleted_test_rows.csv')

    df_testRows.to_csv('deleted_test_rows.csv', index=False)

    logFunctionEnd(report)

    return df


def outputColumnsWithRepeatedData(df):

    funcName = 'Saving Repeated Data as CSV'
    logFunctionStart(funcName)

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

    logFunctionEnd()


def outputReligionData(df):

    funcName = 'Saving Religion Data as CSV'
    logFunctionStart(funcName)

    rels = pd.DataFrame(df['Are you a person of faith?'].unique()).dropna()
    rels.columns = ['VALUES']
    rels.to_csv('data/relgions_for_cleaning.csv', index=False)

    logFunctionEnd()


def cleanData(df, rels, repData):

    funcName = 'Cleaning Data'
    logFunctionStart(funcName)
    report = ''

    print('This will take a while...\n')

    report += 'Replaced any null values with empty string\n'
    df = df.fillna('')

    report += 'Replaced all new line characters with ", "\n'
    df = df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                    value=[", ", ", "],
                    regex=True)

    report += 'Removed commas from ~12 last names\n'
    df.loc[(df['Last Name'].str.contains(',', na=False)) &
           (df['Last Name'] != 'F. Queen, Jr.'),
           'Last Name'] = df['Last Name'].str.replace(',', '')

    report += 'Deleted address fields that are just commas\n'
    df.loc[(df['Address 1'] == ', '), 'Address 1'] = ''
    df.loc[(df['Address 1'] == ','), 'Address 1'] = ''

    report += 'Converted some city names to lower case\n'
    df.loc[df['City'].str.match('^.*[A-Z]$', na=False), 'City'] = \
        df['City'].str.title()

    report += 'Replaced &#039; in city names with apostrophe\n'
    df['City'] == df['City'].str.replace('&#039;', "'")

    report += 'Replaced "0" zip codes with empty string\n'
    df.loc[df['Zip'] == '0', 'Zip'] = ''

    report += 'Fixed typo email address\n'
    df.loc[df['Email'] == 'a..murdock@dsl.pipex.com', 'Email'] = \
        'a.murdock@dsl.pipex.com'

    report += 'Replaced invalid phone numbers with empty string\n'
    df.loc[df['Home Phone'].isin([
           '0', '999', '01', '07', '34', '84', '447511', '447911']),
           'Home Phone'] = ''

    report += 'Deleted the Parliament Phone number (on 28 records)\n'
    df.loc[df['Work Phone'] == '02072193000', 'Work Phone'] = ''

    report += 'Changed date format to be compatible with NationBuilder\n'
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

    report += 'Cleaned religion columns based on manual mapping\n'
    new_df = pd.merge(
        df,
        rels,
        how='left',
        left_on=['Are you a person of faith?'],
        right_on=['Values in Data'])

    df['Are you a person of faith?'] = new_df['Replacement Values']

    report += 'Cleaned columns that have repeated data using manual mapping\n'
    for col in CONFIG['COLS_WITH_REPEATD_DATA']:
        cleanedData = repData[col]
        new_df = pd.merge(
            df,
            cleanedData,
            how='left',
            left_on=['Email'],
            right_on=['Email'])
        df[col] = new_df[col + '_y']

    report += 'Replace strings "Na: and "None" in Organisation with empty string\n'
    colName = 'Organisational/company sign up:Name of Organisation'
    df.loc[df[colName].isin(["None", "Na"]), colName] = ''

    report += 'Replaced any null values with empty string (again!)\n'
    df = df.fillna('')

    logFunctionEnd()

    return df


def outputMultiChoiceLists(df, meta):

    funcName = 'Outputing multiple-choice lists'
    logFunctionStart(funcName)

    customFields = list(meta.loc[
        meta['Custom Field Type?'] == 'Multiple Choice', 'fullColName'])

    for col in customFields:
        customFieldValues = pd.DataFrame(df[col].unique()).dropna()
        customFieldValues.columns = ['VALUES']
        customFieldValues.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                                 CONFIG['CUSTOM_FIELDS_DIRECTORY'] + '/' +
                                 col + '.csv', index=False)

    logFunctionEnd()


def processTags(df, meta):

    funcName = 'Processing Tags'
    logFunctionStart(funcName)

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
        print('Assigning tag "' + tagMapping[colName] + '" where column "' + colName + '" is populated.', end='')
        rowCount = 0
        for i, row in df.loc[(df[colName].notna()) & (df[colName].map(str) != '')].iterrows():
            rowCount += 1
            # There's probably a more pythonic/vectorised way to do this, but
            # I had troube getting this working so am leaving it be. The key
            # was the .copy()
            list = row['tags'].copy()
            if tagMapping[colName] not in list:
                list.append(tagMapping[colName].strip())
            df.at[i, 'tags'] = list
        print(' ' + str(rowCount) + ' rows tagged')

    print()
    df['tags'] = df['tags'].apply(lambda x: ','.join(map(str, x)))

    logFunctionEnd()

    return df


def mapColumns(df, stm):

    funcName = 'Mapping and Merging Columns'
    logFunctionStart(funcName)
    report = ''

    print('This function will output every mapped column, and detail ' +
          'and merging and concatenating that takes place. Scroll through ' +
          'loooong output and check you are happy with the concatenations\n')

    targetColsAlreadyCreated = []

    emailColName = 'Email'

    for i, stmRow in stm.iterrows():

        if stmRow['IN SCOPE'] == 'T':

            if ((stmRow['NB TARGET FIELD'] is np.nan or
                 stmRow['NB TARGET FIELD'] == '') and stmRow['Tag?'] != 'T'):
                raise ValueError('Column not mapped: ' + stmRow['fullColName'])

            # Some rows have no mapping because they are Tags only.
            # Ignore these.
            if (stmRow['NB TARGET FIELD'] is not np.nan and
                    stmRow['NB TARGET FIELD'] != ''):

                fromCol = stmRow['fullColName']
                toCol = stmRow['NB TARGET FIELD']

                print('\n', end='')
                print('Mapping column: ' + str(fromCol) + '\n', end='')
                print('To: ' + str(toCol) + '\n', end='')

                # Some source columns have been mapped to the same target
                # columns. These columns need merging. The rest just get
                # renamed to the new column name
                if toCol not in targetColsAlreadyCreated:

                    df.rename(
                        index=str,
                        columns={fromCol: toCol},
                        inplace=True)

                    targetColsAlreadyCreated.append(toCol)

                    if fromCol == 'Email':
                        emailColName = toCol

                else:

                    # The first occurence of this toCol target column
                    # would have resulted in a normal mapping (i.e. the
                    # fromCol was renamed to the toCol). Subsequent
                    # occurences need to be merged to the existing toCol
                    # and the fromCol dropped

                    print(' - We have already mapped a column to ' +
                          toCol + ', so merge...' + '\n', end='')

                    # Whether our merge requires a concatenation depends on
                    # the two values, so we have to loop through all rows.
                    # This is slooow.

                    numberOfConcatentations = 0

                    for j, dfRow in df.iterrows():

                        fromVal = dfRow[fromCol]
                        toVal = dfRow[toCol]

                        doMerge = True

                        # If the the two values are the same, don't merge
                        if fromVal == toVal:
                            doMerge = False

                        # If the value we're merging in (fromCol) is blank,
                        # don't merge
                        if fromVal is np.nan or fromVal == '' or fromVal == 'nan':
                            doMerge = False

                        if doMerge:

                            # If the target value is blank, don't concat
                            if toVal is np.nan or toVal == '':
                                df.at[j, toCol] = str(fromVal)

                            # Otherwise, we're concatentating, so log the o/p
                            else:
                                numberOfConcatentations += 1
                                newVal = str(toVal) + ', ' + str(fromVal)
                                emailOfConcatRow = df.at[j, emailColName]
                                print('   - ' + emailOfConcatRow + ' ' +
                                      'has values in both cols, ' +
                                      'so concatenate...' + '\n', end='')
                                print('      - Current value: ' + str(toVal) + '\n', end='')
                                print('      - New value: ' + str(fromVal) + '\n', end='')
                                print('      = Merged value: ' + newVal + '\n', end='')

                                df.at[j, toCol] = newVal

                    # We've looped through all rows, so now drop the fromCol
                    df.drop(
                        fromCol,
                        axis=1,
                        inplace=True)

    logFunctionEnd(report)

    return df


def outputData(df):
    funcName = 'outputData'
    logFunctionStart(funcName)
    report = ''

    sampleSize = 10000

    df.to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
              CONFIG['OUTPUT_FILENAME'], index=False)
    df.head(sampleSize).to_csv(CONFIG['DATA_DIRECTORY'] + '/' +
                               CONFIG['SAMPLE_OUTPUT_FILENAME'], index=False)
    report += ("Saved " + str(df.shape[0]) + " rows of data to " +
               CONFIG['OUTPUT_FILENAME'] + '\n')
    report += ("Also saved " + str(sampleSize) + " rows of data to " +
               CONFIG['SAMPLE_OUTPUT_FILENAME'] + '\n')

    logFunctionEnd(report)


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

    df = deleteTestData(df)

    # If you uncomment this, it will overwrite the repeated-values spreadsheet,
    # which you probably don't want to do, given that JCF have already manually
    # cleaned the data in this spreadsheet!
    # outputColumnsWithRepeatedData(df)
    # outputReligionData(df)

    df = cleanData(df, rels, repData)

    outputMultiChoiceLists(df, meta)

    df = processTags(df, meta)

    df = mapColumns(df, meta)

    outputData(df)


if __name__ == "__main__":
    run(sys.argv[1:])
