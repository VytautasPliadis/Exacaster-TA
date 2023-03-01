import os
import pandas as pd
from ydata_profiling import ProfileReport


def main():
    # change current directory
    os.chdir('..')
    path = os.getcwd()

    # generate report for usage.csv
    df = pd.read_csv(path + '\\raw_data\\usage.csv')
    profile = ProfileReport(df, title="Data Profiling Report")
    profile.to_file("Data profiling.html")
    print('OK')


if __name__ == "__main__":
    main()
