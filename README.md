# BI course
I created these 2 scripts for a course on Data ETL for automated Business Intelligence for Marketers.

Sources:
- Google API script: https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
- Facebook API script: https://github.com/facebook/facebook-python-ads-sdk/blob/master/examples/read_objects.py


For the scripts to work you need to authorize Google and Facebook, by
- Google Analytics Access: see steps in https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
- Google Drive Access: https://github.com/maybelinot/df2gspread#access-credentials
- Facebook App and 60 day token: https://github.com/ccasimiro9444/bi_course.git

These scripts query Google Analytics and Facebook data, transform it to a nicer format and return a panads DataFrame, 
then upload the data to a Google Spreadsheet. The scripts return each 2 worksheets with totals and detailed campaign data.

They could be used as a template-starting point for e-commerce firms.
