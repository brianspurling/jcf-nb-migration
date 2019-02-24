# JCF NationBuilder Migration Pipeline

## Overview

This script will take a CSV extract from JCF's legacy CRM system and transform & clean it ready for loading into NationBuilder

The logic is mostly driven from a Source To Target Mapping document - a Google Sheets doc.

# Getting Started

These are the instructions for setting a Mac up to run this script.

## Get the code
* From the main page of [this](https://github.com/brianspurling/jcf-nb-migration) Github repository, click *Clone or download*
* In the Clone with HTTPs section, copy the clone URL for the repository
* Open Terminal on your Mac
* Change the current working directory to the location where you want the code to run (note, this directory will end up containing personal data - so make sure it is not synced to any cloud file systems or backups)  (e.g. `$ cd ~/git/nationbuilder-migration`)
* run `$ git clone <paste the URL copied above>`

## Setup Python
This section assumes you don't already have Python installed. Run `$ python --version`. You probably want to have at least version 3.6. I'm using version 3.7.2. If you don't have it, or need to upgrade, then...
* Make sure you have Brew installed (run `$ brew` from Terminal - you should get example usage if it's installed). If you don't have it, head over [here](https://brew.sh/) and follow the instructions
* Install Python using `$ brew install python`, or update using `$ brew update python`

## Prep the project
* Navigate in Terminal to the directory you cloned the code to (e.g. `$ cd ~/git/nationbuilder-migration`)
* Install the third party packages used by the scrip with `$ pip install -r requirements.txt`
* Get a Google API Key file and add it to the directory containing the code (instructions for creating a Google Sheets API key file [here](https://developers.google.com/sheets/api/guides/authorizing)).
* Once you have set up your GSheets API, open the Google Sheet and share it with the API account
* Run `$ python main.py --setup` - this won't run the pipeline, but will just create the necessary subdirectories. You will see a warning that you don't have a meta text file yet.
* Review the contents of `config.py` (you don't need to change anything, but you need to take note of the `INPUT_FILENAME` option).
* Get an output data file from the legacy system (all columns), name it according to `INPUT_FILENAME` from `config.py`, and place it in the `data` subdirectory

## Data Cleaning Data
* Some of the data cleaning is controlled by external data sources.
* A tab in the STM spreadsheet called RELIGIONS contains a mapping of the religions from the legacy system to a tidy set of religions for NationBuilder
* A separate spreadsheet (REPEATED_DATA_GSHEET_NAME in config.py) contains mappings of repeated values to clean values. This is an issue in the legacy system where checkbox fields seem to have been populated with the same value multiple times. It was easier to clean these manually than code it, hence the mapping spreadsheet  

## Run the code
* Navigate in Terminal to the directory containing the code
* Run `$ python main.py --meta`. The `--meta` argument will tell the pipeline to get the latest meta data from the Source to Target Mapping and save it as a text file. If you make subsequent changes to the Google Sheet, you need to run it with the `--meta` command again.
* The code takes a while to run (and the log outputs are minimal, so it's hard to know what it's doing). It's slow, because there's a lot of manual fixes being applied, which requires constant looping through the entire dataset and comparing values.
* One thing to watch out for in the log output: for columns being merged, the code will tell you about any merges where there were values in both columns. Make sure you're happy that these values will be merged into a single value.
* If you didn't get any errors, there should be a new data file in the directory, named according to the `OUTPUT_FILENAME` option in `config.py`.

## Upload the outputted file to NationBuilder
* ...

# Multiple Choice List Outputs

After the script has run there will be a set of csv files produced (in a directory like root/data/customFieldValues). These are the unique set of values for every column that is intended to be mapped to a multiple choice custom field in NationBuilder. These need entering into NationBuilder manually.
