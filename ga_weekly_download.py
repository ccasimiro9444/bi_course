#Import libraries
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d

# no need to do this.
import json
config_filename = '/your/path/to/file/config.json'
config_file = open(config_filename)
CONFIG = json.load(config_file)
config_file.close()
# this is to hide m id's, tokens, and credentials.

# Define variables
SCOPES_ANALYTICS = ['https://www.googleapis.com/auth/analytics.readonly']
SCOPES_DRIVE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
KEY_FILE_LOCATION = '/your/path/to/file/service_key.json'
VIEW_ID = CONFIG['view_id']
SPREADSHEET = '<your_spreadsheet_url'
WORKSHEETS = ['ga_totals', 'ga_details']

def initialize_drive():
    """Initializes a Drive API service object.

    Returns:
        An authorized Drive API service object.
    """
    credentials_drive = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, 
        SCOPES_DRIVE
        )
    return gspread.authorize(credentials_drive)

def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
        An authorized Analytics Reporting API V4 service object.
    """
    credentials_analytics = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, 
        SCOPES_ANALYTICS
        )
    analytics = build(
        'analyticsreporting', 
        'v4', 
        credentials = credentials_analytics, 
        cache_discovery = False)
    return analytics

def get_week_days(year, week):
    """Calculates dates from iso-years and iso-weeks.

    Args:
        year: An ISO-year.
        week: An ISO-week number.
    Returns:
        The first date of that week, Monday, in dt.date().
    """
    d = dt.date(year, 1, 1)
    if (d.weekday() > 3):
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
        start_date = dt.date(2017, 7, 2)
        return existing, start_date

def get_no_seg_report(analytics, starting_date):
    """Queries the Analytics Reporting API V4 for non-segmented metrics.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        starting_date: The date with which the data queries start.
    Returns:
        The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body = {
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [
                        {
                            'startDate': str(starting_date), 
                            'endDate': str(starting_date + dt.timedelta(days = 6))
                        }],
                    'metrics': [
                        {
                            'expression': 'ga:impressions'
                        },
                        {
                            'expression': 'ga:adClicks'
                        },
                        {
                            'expression': 'ga:adCost',
                            'formattingType': 'FLOAT'
                        }],
                    'dimensions': [
                        {
                            'name': 'ga:campaign'
                        }]
                }]
            }
    ).execute()

def get_seg_report(analytics, starting_date):
    """Queries the Analytics Reporting API V4 for segmented metrics.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        starting_date: The date with which the data queries start.
    Returns:
        The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body = {
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [
                        {
                            'startDate': str(starting_date), 
                            'endDate': str(starting_date + dt.timedelta(days = 6))
                        }],
                    'metrics': [
                        {
                            'expression': 'ga:sessions'
                        },
                        {
                            'expression': 'ga:goal6Completions'
                        },
                        {
                            'expression': 'ga:transactions'
                        },
                        {
                            'expression': 'ga:transactionRevenue',
                            'formattingType': 'FLOAT'
                        }],
                    'dimensions': [
                        {
                            'name': 'ga:campaign'
                        },
                        {
                            'name': 'ga:segment'
                        }
                        ],
                    'segments':[
                        {
                            'dynamicSegment': {
                                'name': 'Paid Sessions',
                                'sessionSegment': {
                                    'segmentFilters': [
                                        {
                                            'not': 'False',
                                            'simpleSegment': {
                                                'orFiltersForSegment': [
                                                    {
                                                        'segmentFilterClauses': [
                                                            {
                                                                'dimensionFilter': {
                                                                    'dimensionName': 'ga:medium',
                                                                    'operator': 'REGEXP',
                                                                    'expressions': ['^(cpc|ppc|cpa|cpm|cpv|cpp)$']
                                                                    }
                                                            }]
                                                    }]
                                                }
                                        }]
                                    }
                                }
                        }]
                }]
            }
        ).execute()

def clean_extracted_data_totals(dict_data, starting_date):
    """Cleans the totals data.

    Args:
        dict_data: A dictionary of extracted data.
        starting_date: The date with which the data queries start.
    Returns:
        A cleanded pandas DataFrame of totals.
    """
    index = []
    for number, _ in enumerate(dict_data['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']):
        index.extend([dict_data['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'][number]['name']])
    iso_year, iso_week = starting_date.isocalendar()[:2]
    return pd.DataFrame(
        dict_data['reports'][0]['data']['totals'][0]['values'], 
        index = index, 
        columns = ['{}-{}'.format(iso_year, iso_week)]
        )

def clean_extracted_data_details(dict_data, starting_date):
    """Cleans the details data.

    Args:
        dict_data: A dictionary of extracted data.
        starting_date: The date with which the data queries start.
    Returns:
        A cleanded pandas DataFrame of campaign details.
    """
    index = ['campaign']
    for number, _ in enumerate(dict_data['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']):
        index.extend([dict_data['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'][number]['name']])
    campaigns_details = []
    for number, _ in enumerate(dict_data['reports'][0]['data']['rows']):
        campaigns_details.extend([
                dict_data['reports'][0]['data']['rows'][number]['dimensions'],
                dict_data['reports'][0]['data']['rows'][number]['metrics'][0]['values']
            ])
    iso_year, iso_week = starting_date.isocalendar()[:2]
    new_index = []
    for i in range(dict_data['reports'][0]['data']['rowCount']):
        for _, j in enumerate(index):
            new_index.append('{}_{}'.format(j, i))
    return pd.DataFrame(
        sum(campaigns_details, []), 
        index = new_index, 
        columns = ['{}-{}'.format(iso_year, iso_week)]
        ), index

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

def loop_adding_weeks_totals(connection, previous_data, starting_date):
    """Merges all the previous and newly extracted data.

    Args:
        connection: The API connection with Google Analytics.
        previous_data: A dataframe from Google Drive, or None.
        starting_date: The date with which the data queries start.
    Returns:
        A cleanded and merged pandas DataFrame of campaign totals.
    """
    start_date = starting_date
    if isinstance(previous_data, pd.DataFrame):
        while start_date + dt.timedelta(days = 6) < dt.date.today(): 
            # Extract data from Google Analytics
            extracted_data_no_seg = get_no_seg_report(connection, start_date)
            extracted_data_seg = get_seg_report(connection, start_date)
            # Transform the extracted data
            transformed_data = clean_extracted_data_totals(
                extracted_data_no_seg,
                start_date
                ).append(
                    clean_extracted_data_totals(
                        extracted_data_seg,
                        start_date
                        ))
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
        # Extract data from Google Analytics
        extracted_data_no_seg = get_no_seg_report(connection, start_date)
        extracted_data_seg = get_seg_report(connection, start_date))
        # Transform the extracted data
        previous_data = clean_extracted_data_totals(
            extracted_data_no_seg,
            start_date
            ).append(
                clean_extracted_data_totals(
                    extracted_data_seg,
                    start_date
                    ))
        # Add another week
        start_date += dt.timedelta(days = 7)
        while start_date + dt.timedelta(days = 6) < dt.date.today(): 
            # Extract data from Google Analytics
            extracted_data_no_seg = get_no_seg_report(connection, start_date)
            extracted_data_seg = get_seg_report(connection, start_date)
            # Transform the extracted data
            transformed_data = clean_extracted_data_totals(
                extracted_data_no_seg,
                start_date
                ).append(
                    clean_extracted_data_totals(
                        extracted_data_seg,
                        start_date
                        ))
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

def loop_adding_weeks_details(connection, previous_data, starting_date):
    """Merges all the previous and newly extracted data.

    Args:
        connection: The API connection with Google Analytics.
        previous_data: A dataframe from Google Drive, or None.
        starting_date: The date with which the data queries start.
    Returns:
        A cleanded and merged pandas DataFrame of campaign details.
    """
    start_date = starting_date
    if isinstance(previous_data, pd.DataFrame):
        while start_date + dt.timedelta(days = 6) < dt.date.today(): 
            # Extract data from Google Analytics
            extracted_data_no_seg = get_no_seg_report(connection, start_date)
            # Transform the extracted data
            transformed_data, index = clean_extracted_data_details(extracted_data_no_seg, start_date)
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
        # Extract data from Google Analytics
        extracted_data_no_seg = get_no_seg_report(connection, start_date)
        # Transform the extracted data
        previous_data, index = clean_extracted_data_details(extracted_data_no_seg, start_date)
        # Add another week
        start_date += dt.timedelta(days = 7)
        while start_date + dt.timedelta(days = 6) < dt.date.today(): 
            # Extract data from Google Analytics
            extracted_data_no_seg = get_no_seg_report(connection, start_date)
            # Transform the extracted data
            transformed_data, index = clean_extracted_data_details(extracted_data_no_seg, start_date)
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
    # Authorize credentials with Google Drive and Google Analytics
    gc = initialize_drive()
    analytics = initialize_analyticsreporting()

    # Download existing or make dataframe and the last week
    existing_totals, start_date_totals = download_df(WORKSHEETS[0])
    existing_details, start_date_details = download_df(WORKSHEETS[1])

    # Getting the sweet data
    all_data_totals = loop_adding_weeks_totals(analytics, existing_totals, start_date_totals)
    all_data_details = loop_adding_weeks_details(analytics, existing_details, start_date_details)
    
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
    