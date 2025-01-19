import pandas as pd
from datetime import datetime, timedelta
import requests
import re
import gspread
import os
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib

def lambda_handler(event, context):

    def get_dates():
        today = datetime.now().date()
        six_months_ago = today - timedelta(days=180)
        today_formatted = today.strftime("%Y%m%d")
        six_months_ago_formatted = six_months_ago.strftime("%Y%m%d")
        return six_months_ago_formatted, today_formatted

    def de_list(cell):
        if isinstance(cell, list):
            return ', '.join(map(str, cell))
        else:
            return cell

    def send_email(subject, body, attachment_path=None):

        sender_email = os.environ.get('MY_DATA_SCIENCE_EMAIL')
        sender_password = os.environ.get('MY_DATA_SCIENCE_EMAIL_PASSWORD')
        recipient_email = os.environ.get('MY_PERSONAL_EMAIL')

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        if attachment_path:
            attachment = MIMEApplication(open(attachment_path, 'r').read())
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
            msg.attach(attachment)

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())

    try:

        # Initialize the API request
        
        six_months_ago, today = get_dates()
        url = f'https://api.fda.gov/drug/event.json?search=(receivedate:[{six_months_ago}+TO+{today}])+AND+(patient.patientonsetage:[0+TO+17])&limit=1000'
        response = requests.get(url)

        link_header = response.headers.get('Link', '')
        link_header = re.search(r'<(.*?)>', link_header).group(1)

        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            df = pd.DataFrame(results)
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
        
        # Repeat the API request until there's no more pages. This is the approach recommended here: https://open.fda.gov/apis/paging/.

        while link_header:
            url = link_header
            response = requests.get(url)

            link_header = response.headers.get('Link', '')
            if link_header:
                link_header = re.search(r'<(.*?)>', link_header).group(1)

            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                temp = pd.DataFrame(results)
                df = pd.concat([df, temp], axis=0)
                print(f'Dataframe length: {df.shape[0]:,}; Unique safetyreportids: {df.safetyreportid.nunique():,}')
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
        
        # Flatten the patient data

        df = df.reset_index(drop=True)
        df = pd.concat([df, pd.json_normalize(df['patient'])], axis=1).drop(columns=['patient'])

        # Flatten the drug data. There are multiple drugs per patient, so the dataset is now at the patient-drug level.

        df = df.explode('drug')
        df = df.reset_index(drop=True)
        df = pd.concat([df, pd.json_normalize(df['drug'])], axis=1).drop(columns=['drug'])

        # De-list cells formatted as lists.
        
        for column in df.columns:
            df[column] = df[column].apply(lambda cell: de_list(cell))
        
        # Keep only columns of interest to reduce file size.

        df = df[[
        'safetyreportversion',
        'safetyreportid',
        'primarysourcecountry',
        'occurcountry',
        'transmissiondateformat',
        'transmissiondate',
        'reporttype',
        'serious',
        'seriousnessdeath',
        'seriousnesslifethreatening',
        'seriousnesshospitalization',
        'seriousnessdisabling',
        'seriousnesscongenitalanomali',
        'seriousnessother',
        'receivedateformat',
        'receivedate',
        'receiptdateformat',
        'receiptdate',
        'fulfillexpeditecriteria',
        'companynumb',
        'duplicate',
        'authoritynumb',
        'patientonsetage',
        'patientonsetageunit',
        'patientagegroup',
        'patientsex',
        'patientweight',
        'summary.narrativeincludeclinical',
        'drugcharacterization',
        'medicinalproduct',
        'drugdosagetext',
        'drugdosageform',
        'drugindication',
        'activesubstance.activesubstancename',
        'openfda.application_number',
        'openfda.brand_name',
        'openfda.generic_name',
        'openfda.manufacturer_name',
        'openfda.product_type',
        'openfda.route',
        'openfda.substance_name',
        'openfda.pharm_class_epc',
        'actiondrug',
        'drugadditional',
        'drugauthorizationnumb',
        'openfda.pharm_class_moa',
        'openfda.pharm_class_cs'
        ]]

        # Add a timestamp to track the upload time.

        df['batch_dttm'] = pd.to_datetime(datetime.now()) - pd.Timedelta(hours=8)

        # Convert all values to strings to facilitate upload to Google Sheets.

        df = df.astype(str)

        # Truncate cells to less than 50,000 characters to facilitate upload to Google Sheets.

        truncate_to_max_length = lambda cell: str(cell)[:49999]
        df = df.applymap(truncate_to_max_length)

        # Export the data to Google Sheets. Useful resources:
        # - https://ploomber.io/blog/gsheets/
        # - https://www.youtube.com/watch?v=bu5wXjz2KvU
        # - https://medium.com/@febyanitasari/how-to-connect-pandas-dataframe-to-looker-studio-google-data-studio-4e4dbc86e322

        sa = gspread.service_account(filename="credentials.json")
        sheet = sa.open('drug_adverse_events_children')
        worksheet = sheet.worksheet('drug_adverse_events_children')

        batch_size = 10000
        num_batches = (len(df) // batch_size) + 1

        worksheet.clear()

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            df_batch = df.iloc[start_idx:end_idx]
            if i == 0:
                worksheet.update([df_batch.columns.values.tolist()] + df_batch.values.tolist())
            else:
                worksheet.append_rows(df_batch.values.tolist())
            print(f'Batch {i + 1} of {num_batches} uploaded.')

        # Send an email

        send_email('Notebook Execution Successful: drug_adverse_events_children', 'The Jupyter notebook drug_adverse_events_children executed successfully.')

        print('Notebook execution successful.')
    
    except Exception as e:

        # Send an email

        send_email('Notebook Execution Failed: drug_adverse_events_children', 'There was an error executing the Jupyter notebook drug_adverse_events_children.')

        print(f"Error: {e}")