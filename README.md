# Drug Adverse Events for Children

Drug adverse events are an important preventable cause of morbidity and mortality.

This project used Python, Google Sheets, AWS Lambda, and Looker Studio to surface insights on drug adverse events for children in the United States.

Key steps in this project:
- Used the `requests` module to get JSON data from the API of the [FDA Adverse Event Reporting System](https://open.fda.gov/apis/drug/event/)
- Converted the JSON into a `pandas` dataframe and cleaned the dataset
- Used the `gspread` module to export the data to Google Sheets
- Used the `email` and `smtplib` modules with secure environment variables containing credentials to send emails indicating if code execution succeeded or failed
- Formatted code for AWS Lambda and scheduled a trigger to run the job automatically on a regular basis
- Visualized data in an interactive Google Looker Studio report that can be filtered by manufacturer, drug, and other factors [here](https://lookerstudio.google.com/s/q0aqioaWLK4)