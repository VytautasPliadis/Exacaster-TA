import os
import pandas as pd


def split_csv(file_name, data_path, rows_per_file):
    # Read the CSV file into a pandas dataframe
    df = pd.read_csv(file_name, header=None)
    headers = ["customer_id", "event_start_time", "event_type", "rate_plan_id", "billing_flag_1", "billing_flag_2",
               "duration", "charge", "month"]
    df.columns = headers
    # Remove duplicate rows and keep only one
    df = df.drop_duplicates(keep='first')

    # Split the dataframe into chunks and write them to separate files
    for i, (group_name, group_data) in enumerate(df.groupby(df.index // rows_per_file)):
        file_name = f'usage_split_{i}.csv'
        csv_path = os.path.join(data_path, file_name)
        group_data.to_csv(csv_path, index=False)


def main():
    # make dir for split csv
    os.chdir('..')
    path = os.getcwd()
    if not os.path.exists(path + '\\processed_data\\split_csv'):
        os.mkdir(path + '\\processed_data\\split_csv')

    # split csv
    split_csv(path + '\\raw_data\\usage.csv', path + '\\processed_data\\split_csv', 10000)


if __name__ == "__main__":
    main()
