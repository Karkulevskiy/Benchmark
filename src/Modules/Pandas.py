import os.path
import pandas as pd
import time
import statistics


class Pandas:
    def __init__(self, number_of_starts, path_to_file_csv):
        self.data = []
        self.number_of_starts = number_of_starts
        self.path_to_file_csv = path_to_file_csv

    def start_test(self):
        if not os.path.exists(self.path_to_file_csv):
            print("File not exist")
            return

        df = pd.read_csv(self.path_to_file_csv)
        for i in range(4):
            static_data = []
            for j in range(1):
                if i == 0:
                    start = time.time()
                    selected_df = df[['VendorID']]
                    grouped_df = selected_df.groupby('VendorID')
                    final_df = grouped_df.size().reset_index(name='counts')
                    static_data.append(time.time() - start)
                elif i == 1:
                    start = time.time()
                    selected_df = df[['passenger_count', 'total_amount']]
                    grouped_df = selected_df.groupby('passenger_count')
                    final_df = grouped_df.mean().reset_index()
                    static_data.append(time.time() - start)
                elif i == 2:
                    start = time.time()
                    selected_df = df[['passenger_count', 'tpep_pickup_datetime']]
                    selected_df['year'] = pd.to_datetime(
                        selected_df.pop('tpep_pickup_datetime'),
                        format='%Y-%m-%d %H:%M:%S').dt.year
                    grouped_df = selected_df.groupby(['passenger_count', 'year'])
                    final_df = grouped_df.size().reset_index(name='counts')
                    static_data.append(time.time() - start)
                elif i == 3:
                    start = time.time()
                    selected_df = df[[
                        'passenger_count',
                        'tpep_pickup_datetime',
                        'trip_distance']]
                    selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
                    selected_df['year'] = pd.to_datetime(
                        selected_df.pop('tpep_pickup_datetime'),
                        format='%Y-%m-%d %H:%M:%S').dt.year
                    grouped_df = selected_df.groupby([
                        'passenger_count',
                        'year',
                        'trip_distance'])
                    final_df = grouped_df.size().reset_index(name='counts')
                    final_df = final_df.sort_values(
                        ['year', 'counts'],
                        ascending=[True, False])
                    static_data.append(time.time() - start)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")
        self.print_time("Pandas")

    def print_time(self, db_name):
        print(f"{db_name}: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write(f"{db_name}:\n" + "".join(self.data))