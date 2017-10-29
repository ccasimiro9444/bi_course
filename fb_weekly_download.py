# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Extracts campaign statistics.
"""


#Import libraries
from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from facebookads.adobjects.adaccountuser import AdAccountUser
from facebookads.adobjects.campaign import Campaign
import pandas as pd
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d
from time import sleep

# no need to do this.
import json
config_filename = '/your/path/to/file/config.json'
config_file = open(config_filename)
CONFIG = json.load(config_file)
config_file.close()
# this is to hide m id's, tokens, and credentials.

# Define variables
APP_ID = CONFIG['app_id']
APP_SECRET = CONFIG['app_secret']
ACCESS_TOKEN = CONFIG['access_token']
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
KEY_FILE_LOCATION = '/your/path/to/file/client_secrets_key.json'
INSIGHT_FIELDS = ['impressions', 'clicks', 'spend']
SPREADSHEET = '<your_spreadsheet_url>'
WORKSHEETS = ['facebook_totals', 'facebook_details']

def initialize_facebook():
    """Initializes a Facebook API session object.

    Returns:
        An authorized Facebook API session object.
    """
    session = FacebookSession(APP_ID, APP_SECRET, ACCESS_TOKEN)
    return FacebookAdsApi(session)

def initialize_drive():
    """Initializes a Drive API service object.

    Returns:
        An authorized Drive API service object.
    """
    credentials_drive = ServiceAccountCredentials.from_json_keyfile_name(
    KEY_FILE_LOCATION, 
    SCOPE
    )
    return gspread.authorize(credentials_drive)

def get_week_days(year, week):
    """Calculates dates from iso-years and iso-weeks.

    Args:
        year: An ISO-year.
        week: An ISO-week number.
    Returns:
        The first date of that week, Monday, in dt.datetime.date().
    """
    d = dt.date(year, 1, 1)
    if(d.weekday() > 3):
        d = d + dt.timedelta(7 - d.weekday())
    else:
        d = d - dt.timedelta(d.weekday())
    dlt = dt.timedelta(days = (week - 1) * 7)
    return d + dlt #,  d + dlt + dt.timedelta(days = 6)

def download_df(worksheet_name):
    """Downloading an existing worksheet from Google Drive.

    Args:
        worksheet_name: The correct worksheet name on the spreadsheet.
    Returns:
        The existing worksheet data, or None;
        and the first date of the next week's query.
    """
    try:
        existing = g2d.download(gfile = SPREADSHEET, wks_name = worksheet_name, col_names = True, row_names = True)
        start_date = get_week_days(
            int(existing.columns[-1].split('-')[0]), 
            int(existing.columns[-1].split('-')[1])
            ) + dt.timedelta(days = 7)
        return existing, start_date
    except RuntimeError:
        existing = None
        start_date = dt.date(2016, 8, 29)
        return existing, start_date

def campaign_stats(ad_account, starting_date, ad_fields):
    """Extracting Facebook data.

    Args:
        ad_account: the ad account to take data.
        starting_date: the starting date of the campaign to take data.
        ad_fields: the metrics of the campaign to take data.
    Returns:
        A dictionary of Facebook campaign stats.
    """
    insight_params = {
        'time_range': {
            'since': str(starting_date),
            'until': str(starting_date + dt.timedelta(days = 6))
            }
        }
    stats_data_dict = {}
    for campaign in ad_account.get_campaigns(fields = [Campaign.Field.name]):
        # Slow down your requests, if you have many ad campaigns.
        sleep(5)
        for stat in campaign.get_insights(fields = ad_fields, params = insight_params):
            for statfield in stat:
                if campaign[campaign.Field.name] not in stats_data_dict.keys():
                    stats_data_dict[campaign[campaign.Field.name]] = {statfield: stat[statfield]}
                else:
                    stats_data_dict[campaign[campaign.Field.name]][statfield] = stat[statfield]
    return stats_data_dict

def clean_extracted_data_totals(dict_data, starting_date):
    """Cleans extracted Facebook campaign stats.

    Args:
        dict_data: the ad account data in dictionary format.
        starting_date: the starting date of the campaign to take data.
    Returns:
        A totaled pandas DataFrame of the extracted Facebook campaign stats.
    """
    stats_dataframe = pd.DataFrame.from_dict(dict_data)
    iso_year, iso_week = starting_date.isocalendar()[:2]
    stats_dataframe.drop(stats_dataframe.index[stats_dataframe.index == 'date_start'], inplace = True)
    stats_dataframe.drop(stats_dataframe.index[stats_dataframe.index == 'date_stop'], inplace = True)
    stats_dataframe = stats_dataframe.astype(float)
    stats_dataframe = stats_dataframe.sum(axis = 1)
    return stats_dataframe.to_frame('{}-{}'.format(iso_year, iso_week))

def clean_extracted_data_details(dict_data, starting_date):
    """Cleans extracted Facebook campaign stats.

    Args:
        dict_data: the ad account data in dictionary format.
        starting_date: the starting date of the campaign to take data. 
    Returns:
        A detailed pandas DataFrame of the extracted Facebook campaign stats.
    """
    stats_dataframe = pd.DataFrame.from_dict(dict_data)
    iso_year, iso_week = starting_date.isocalendar()[:2]
    stats_dataframe.drop(stats_dataframe.index[stats_dataframe.index == 'date_start'], inplace = True)
    stats_dataframe.drop(stats_dataframe.index[stats_dataframe.index == 'date_stop'], inplace = True)
    titles = pd.DataFrame(stats_dataframe.columns).T.rename(columns = pd.DataFrame(stats_dataframe.columns).T.loc[0])
    titles.index = ['campaign']
    stats_dataframe = titles.append(stats_dataframe)
    one_column = stats_dataframe.iloc[:, 0]
    for number in range(1, len(stats_dataframe.columns)):
        one_column = one_column.append(stats_dataframe.iloc[:, number])
    new_index = []
    for i, _ in enumerate(titles):
        for _, j in enumerate(stats_dataframe.index):
            new_index.append('{}_{}'.format(j, i))
    one_column.index = new_index
    return one_column.to_frame('{}-{}'.format(iso_year, iso_week)), stats_dataframe.index

def sort_data(data, metrics):
    """Sort data similar to the totals.

    Args:
        data: A pandas DataFrame.
        metrics: The metrics of a campaign.
    Returns:
        A sorted pandas DataFrame with an index list similar to totals.
    """
    number_of_campaigns = int(len(data.index)/len(metrics))
    index = []
    for add_number, _ in enumerate(metrics):
        index += [int(data.index[row].split('_')[1] + str(add_number)) for row in range(number_of_campaigns)]
    data.index = index
    data.sort_index(inplace = True)
    new_index = []
    for i in range(number_of_campaigns):
        for _, j in enumerate(sorted(metrics)):
            new_index.append('{}_{}'.format(j, i))
    data.index = new_index
    return data

def loop_adding_weeks_totals(ad_account, previous_data, starting_date, ad_fields):
    """Merges all the previous and newly extracted data.

    Args:
        ad_account: the ad account to take data.
        previous_data: A dataframe from Google Drive, or None.
        starting_date: The date with which the data queries start.
        ad_fields: the metrics of the campaign to take data. 
    Returns:
        A cleanded and merged pandas DataFrame of campaign totals.
    """
    start_date = starting_date
    if isinstance(previous_data, pd.DataFrame):
        while start_date + dt.timedelta(days = 6) < dt.date(2016, 9, 5): #dt.date.today()
            # Extract data from Facebook
            extracted_data = campaign_stats(ad_account, start_date, ad_fields)
            # Transform the extracted data
            transformed_data = clean_extracted_data_totals(extracted_data, start_date)
            # Add another week
            start_date += dt.timedelta(days = 7)
            # Merge existing datat with new column
            previous_data = pd.merge(
                previous_data, 
                transformed_data,
                left_index = True,
                right_index = True
                )
        return previous_data
    else:
        # Extract data from Facebook
        extracted_data = campaign_stats(ad_account, start_date, ad_fields)
        # Transform the extracted data
        previous_data = clean_extracted_data_totals(extracted_data, start_date)
        # Add another week
        start_date += dt.timedelta(days = 7)
        while start_date + dt.timedelta(days = 6) < dt.date(2016, 9, 5): #dt.date.today()
            # Extract data from Facebook
            extracted_data = campaign_stats(ad_account, start_date, ad_fields)
            # Transform the extracted data
            transformed_data = clean_extracted_data_totals(extracted_data, start_date)
            # Add another week
            start_date += dt.timedelta(days = 7)
            # Merge existing datat with new column
            previous_data = pd.merge(
                previous_data, 
                transformed_data,
                left_index = True,
                right_index = True
                )
        return previous_data 

def loop_adding_weeks_details(ad_account, previous_data, starting_date, ad_fields):
    """Merges all the previous and newly extracted data.

    Args:
        ad_account: the ad account to take data.
        previous_data: A dataframe from Google Drive, or None.
        starting_date: The date with which the data queries start.
        ad_fields: the metrics of the campaign to take data. 
    Returns:
        A cleanded and merged pandas DataFrame of campaign details.
    """
    start_date = starting_date
    if isinstance(previous_data, pd.DataFrame):
        while start_date + dt.timedelta(days = 6) < dt.date(2016, 9, 5): #dt.date.today()
            # Extract data from Facebook
            extracted_data = campaign_stats(ad_account, start_date, ad_fields)
            # Transform the extracted data
            transformed_data, index = clean_extracted_data_details(extracted_data, start_date)
            # Add another week
            start_date += dt.timedelta(days = 7)
            # Merge existing datat with new column
            previous_data = pd.merge(
                previous_data, 
                transformed_data,
                how = 'outer',
                left_index = True,
                right_index = True
                )
        return sort_data(previous_data, index)
    else:
        # Extract data from Facebook
        extracted_data = campaign_stats(ad_account, start_date, ad_fields)
        # Transform the extracted data
        previous_data, index = clean_extracted_data_details(extracted_data, start_date)
        # Add another week
        start_date += dt.timedelta(days = 7)
        while start_date + dt.timedelta(days = 6) < dt.date(2016, 9, 5): #dt.date.today()
            # Extract data from Facebook
            extracted_data = campaign_stats(ad_account, start_date, ad_fields)
            # Transform the extracted data
            transformed_data, index = clean_extracted_data_details(extracted_data, start_date)
            # Add another week
            start_date += dt.timedelta(days = 7)
            # Merge existing datat with new column
            previous_data = pd.merge(
                previous_data, 
                transformed_data,
                how = 'outer',
                left_index = True,
                right_index = True
                )
        return sort_data(previous_data, index) 


def main():
    # Initialize API access
    FacebookAdsApi.set_default_api(initialize_facebook())

    # Authorize credentials with Google Drive
    gc = initialize_drive()

    # Get account connected to the user
    # [3] may not be your account, find the right account, try [0] first
    my_account = AdAccountUser(fbid = 'me').get_ad_accounts()[3]

    # Download existing or make dataframe and the last week
    existing_totals, start_date_totals = download_df(WORKSHEETS[0])
    existing_details, start_date_details = download_df(WORKSHEETS[1])

    # Getting the sweet data
    all_data_totals = loop_adding_weeks_totals(my_account, existing_totals, start_date_totals, INSIGHT_FIELDS)
    all_data_details = loop_adding_weeks_details(my_account, existing_details, start_date_details, INSIGHT_FIELDS)

    # Upload the transformed data to Google Sheets
    d2g.upload(
        df = all_data_totals, 
        gfile = SPREADSHEET, 
        wks_name = WORKSHEETS[0]
        )
    d2g.upload(
        df = all_data_details, 
        gfile = SPREADSHEET, 
        wks_name = WORKSHEETS[1]
        )

if __name__ == "__main__":
    main()
