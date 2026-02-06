import sqlite3
import csv
import os


def export_data_to_csv(db_file, facility_table, boundary_table, output_folder):

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    tables = [facility_table, boundary_table]

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Export data to CSV files
    for table in tables:
        csv_filename = os.path.join(output_folder, f'{table}.csv')
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Fetch data from the specified table
            cursor.execute(f'SELECT * FROM {table}')

            csv_writer.writerow(
                [description[0] for description in cursor.description])  # Write headers

            csv_writer.writerows(cursor)  # Write data directly to CSV file

    # Close the database connection
    conn.close()


if __name__ == '__main__':
    # Specify the SQLite database file and table names

    db_file = 'niassa-full-ingestion-20Jan-2026.db'
    facility_table = 'egov_microplan_facilities'
    boundary_table = 'egov_microplan_boundaries'
    output_folder = 'files/output/niassa-full-ingestion-20Jan-2026'

    # Call the export function
    export_data_to_csv(db_file, facility_table,
                       boundary_table, output_folder)
