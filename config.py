
CONFIG = {
    'INPUT_FILENAME': 'export_03_15Feb.csv',
    'OUTPUT_FILENAME': 'data_prepped_for_nb.csv',
    'EXPECTED_ROW_COUNT': 68589,
    'EXPECTED_COL_COUNT': 297,
    'DATA_DIRECTORY': 'data',
    'RELIGIONS_MAP_TMP_FILENAME': 'religion_map.csv',
    'CUSTOM_FIELDS_DIRECTORY': 'customFieldValues',
    'META_DATA_GSHEET_NAME': 'JCF - Source to Target Mapping',
    'REPEATED_DATA_GSHEET_NAME': 'JCF - Repeated Data Output',
    'GOOGLE_API_KEY_FILE': 'jcf_google_api_key_file.json',
    'GOOGLE_API_SCOPE': [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'],
    'META_DATA_TMP_FILENAME': 'meta_data.csv',
    'COLS_WITH_REPEATD_DATA': [
        'Organisational/company sign up:Region',
        'Schools 2018:Key Contact Name',
        'Schools 2018:Region',
        '2018 Supporter Pack:Are you planning on attending or organising an event?',
        '2018 Supporter Pack:What kind of Get Together will you organise?',
        'Organisational/company sign up:What is your reach?',
        'PACK - Form 2 - Who With:Who would you most like to have a get together with? Letting us know will mean we can give you better support setting up your event.',
        'PLEDGE 1 TGGT Website:Will you pledge to do something -- big or small -- to bring your local community together?',
        'PLEDGE 2 TGGT Website:Which of these activities appeals to you most?',
        'Christmas Sign Up:Checkbox',
        'PACK - Form 1 - Details:What type of pack would you like?']
}
