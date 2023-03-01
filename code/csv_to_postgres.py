import logging
import pandas as pd
import os
import psycopg2
from psycopg2 import Error



# Set up logging
logging.basicConfig(filename='ETL_errors.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    # connect to the Postgres database
    conn = psycopg2.connect(
        host="localhost",
        database="telco_usage",
        user="postgres",
        password="testadmin"
    )
    cur = conn.cursor()

    # create table to store csv data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS landing_zone (
            customer_id INTEGER NOT NULL,
            event_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
            event_type VARCHAR(5) NOT NULL,
            rate_plan_id INTEGER NOT NULL,
            billing_flag_1 SMALLINT NOT NULL,
            billing_flag_2 BOOLEAN NOT NULL, 
            duration SMALLINT NOT NULL,
            charge REAL NOT NULL,
            month VARCHAR(7) NOT NULL
            )
    """)
    conn.commit()

    # change current directory
    os.chdir('..')

    # define the folder containing the CSV files
    csv_folder = "processed_data\\split_csv"
    # get a list of all CSV files in the folder, sorted in ascending order
    csv_files = sorted([f for f in os.listdir(csv_folder) if f.endswith(".csv")])

    # loop through the list of CSV files
    csv_qty = len(csv_files)
    count = 0
    for csv_file in csv_files:
        # define the path to the CSV file
        csv_path = os.path.join(csv_folder, csv_file)
        # Read CSV file
        df = pd.read_csv(csv_path)

        # insert rows into table
        for i, row in df.iterrows():
            try:
                cur.execute(
                    "INSERT INTO usage (customer_id, event_start_time, event_type, rate_plan_id, billing_flag_1, "
                    "billing_flag_2, duration, charge, month) VALUES (%s, %s, %s, %s, %s, %s::boolean, "
                    "%s, %s, %s);",
                    (row['customer_id'], row['event_start_time'], row['event_type'], row['rate_plan_id'],
                     row['billing_flag_1'], row['billing_flag_2'], row['duration'], row['charge'], row['month']))

            except Error as e:
                conn.rollback()
                print(f"Error inserting {csv_file}: \n{row} \n{e}")
                logger.error(f"Error inserting {csv_file}: \n{row} \n{e}")
                continue
        conn.commit()
        count += 1
        print(f"{count} of {csv_qty} {csv_file}")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
