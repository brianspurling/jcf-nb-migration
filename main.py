import pandas as pd
import numpy as np

from config import CONFIG


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


def filterToInscopeColumns(df):
    # Some of the column names have carriage returns in, which
    # is a problem for matching to our list of in-scope columns.
    allCols = df.columns.str.replace('\n', '')
    df.columns = allCols
    df = df[CONFIG['INSCOPE_COLUMNS']]
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

    # Remove commas from ~12 last names
    df.loc[(df['Last Name'].str.contains(',', na=False)) & (df['Last Name'] != 'F. Queen, Jr.'),'Last Name'] = df['Last Name'].str.replace(',', '')

    # Delete address fields that are just commas
    df.loc[(df['Address 1'] == ', '), 'Address 1'] = np.nan
    df.loc[(df['Address 1'] == ','), 'Address 1'] = np.nan

    # Lower case some city names
    df.loc[df['City'].str.match('^.*[A-Z]$', na=False), 'City'] = df['City'].str.title()

    # Manually fix some city names
    df.loc[df['City'] == 'St. Mary&#039;s Ward', 'City'] = "St. Mary's Ward"

    # Replace seven "0" phone numbers with nan
    # TODO: change this to regex, there are 0000 etc too
    df.loc[df['Home Phone'] == '0', 'Home Phone'] = np.nan

    return df


def mapColumns(df):
    df.rename(
        index=str,
        columns=CONFIG['COLUMN_MAPPINGS'],
        inplace=True)
    return df



def outputData(df):

    df.to_csv(CONFIG['OUTPUT_FILENAME'], index=False)
    print("Saved data to " + CONFIG['OUTPUT_FILENAME'])


def run():

    df = loadData()
    df = filterToInscopeColumns(df)
    df = deleteTestData(df)

    df = cleanData(df)

    df = mapColumns(df)

    outputData(df)


run()
