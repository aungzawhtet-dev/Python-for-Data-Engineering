# Mini-project: Combine multiple CSVs in a folder → remove duplicates → save to output.csv

import os   # for working with file paths
import csv  # for reading/writing CSV files


def combine_csvs(input_folder, output_file):
    all_rows = []   # list to hold all rows from all CSVs
    seen = set()    # set to track seen rows for deduplication
    header = None   # to store header once

    try:
        # loop through all files in the input folder
        for file in os.listdir(input_folder):
            if file.endswith(".csv"):   
                file_path = os.path.join(input_folder, file) # combine foler path + file name
                print(f"Reading file: {file_path}")

                with open(file_path, "r", newline="") as csvfile: # Without newline="", Python and the csv module can conflict, causing extra blank lines when reading or writing CSVs.
                    reader = csv.reader(csvfile)
                    file_header = next(reader)  # reads the first line (the header) and stores it in file_header

                    # store header only once (from the first file)
                    if header is None:
                        header = file_header

                    # process rows
                    for row in reader:
                        row_tuple = tuple(row)  # convert list → tuple (hashable for set)
                        if row_tuple not in seen:
                            seen.add(row_tuple) # add tuple to seen
                            all_rows.append(row) # append row to all_rows

        # write combined rows to output file
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if header:   # only write if header was found
                writer.writerow(header)
            writer.writerows(all_rows)

        print(f" Combined CSV saved to: {output_file}")

    except Exception as e:
        print(f" An error occurred: {e}")


# Example usage
if __name__ == "__main__":
    combine_csvs("data_folder", "output.csv")
    
    """
    With the if __name__ == "__main__": guard, the function only runs if the file is executed directly, not when imported.
    But using the if __name__ == "__main__": guard is considered a best practice in Python projects.
    """
    
    
