import os
import pandas as pd
from tdda.constraints import verify_df
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Set up email configuration
smtp_username = os.environ.get('MAIL_FROM')
to_email = os.environ.get('MAIL_TO')
smtp_password = os.environ.get('SMTP_PASSWORD')
smtp_server = 'smtp.gmail.com'
smtp_port = 587


def main():
    # change current directory
    os.chdir('..')
    path = os.getcwd()

    header_list = ['customer_id', 'event_start_time', 'event_type', 'rate_plan_id', 'billing_flag_1', 'billing_flag_2',
                   'duration', 'charge', 'month']
    df = pd.read_csv(path + '\\raw_data\\usage.csv', names=header_list)

    constraints = (path + '\\processed_data\\usage.tdda')
    verification = verify_df(df, constraints)
    print(verification)

    fail = verification.failures
    if fail != 0:
        # Set up the email message
        sender = "python.data.quality@gmail.com"
        receiver = "vytautas.pliadis@gmail.com"
        subject = "Data quality alert!"
        body = f"Usage data file constraints are failing: \n{verification}"
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        # Log in to the SMTP server and send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender, receiver, msg.as_string())


if __name__ == "__main__":
    main()
