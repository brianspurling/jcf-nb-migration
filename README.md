# JCF NationBuilder Migration

This script will take a CSV extract from JCF's legacy CRM system and
transform & clean it ready for loading into NationBuilder

The logic is mostly driven from a Source To Target Mapping document - a Google
Sheets doc.

To use, download or clone the repository, place a CSV containing the legacy
system's data in the same directory, set the values in `config.py` accordingly,
set up the Google Sheet, and run python `main.py` from the command line

To set up the Google Sheet you need a Google API key file in the directory (set
the filename in `config.py`). The Google Sheet must be shared to the API
account.
