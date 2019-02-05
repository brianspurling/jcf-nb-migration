
CONFIG = {
    'INPUT_FILENAME': 'export_01_sample.csv',
    'OUTPUT_FILENAME': 'data_prepped_for_nb.csv',
    'EXPECTED_ROW_COUNT': 1000,  # 93954,
    'EXPECTED_COL_COUNT': 297,
    'DATA_DIRECTORY': 'data',
    'CUSTOM_FIELDS_DIRECTORY': 'customFieldValues',
    'META_DATA_GSHEET_NAME': 'JCF - Source to Target Mapping',
    'GOOGLE_API_KEY_FILE': 'jcf_google_api_key_file.json',
    'GOOGLE_API_SCOPE': [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'],
    'META_DATA_TMP_FILENAME': 'meta_data.csv',
}
