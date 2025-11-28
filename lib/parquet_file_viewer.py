import pandas as pd
import os

# --- Pandas Display Settings ---
# For a DataFrame with 10 columns, these settings will help prevent truncation.
pd.set_option('display.max_columns', None)  # Ensure all 10 columns are shown
pd.set_option('display.width', 1000)        # Adjust terminal width to avoid line wrapping
pd.set_option('display.max_colwidth', 50)   # Adjust max width of data in columns
pd.set_option('display.max_rows', 100)      # Set a reasonable number of rows to display

# --- File Path ---
# Make sure this path is correct
file_path = "/Users/kurtmatthewamodia/Downloads/weekend_daily_busiest_hours_df.parquet"

# --- Read and Inspect DataFrame ---
if not os.path.exists(file_path):
    print(f"Error: File not found at '{file_path}'")
else:
    print(f"Reading file: {file_path}")
    df = pd.read_parquet(file_path)

    print("\n--- DataFrame Info ---")
    print("This shows data types and non-null counts for each column.")
    df.info()

    # print("\n\n--- First 10 Rows (Head) ---")
    # print(df.head(10))

    # print("\n\n--- Last 10 Rows (Tail) ---")
    # print(df.tail(10))

    # --- To view the FULL DataFrame ---
    # Uncomment the lines below. Be aware that this will print all 4000+ rows
    # to your console, which may be slow or hang your terminal.
    #
    print("\n\n--- Full DataFrame ---")
    pd.set_option('display.max_rows', None) # Temporarily allow all rows
    print(df)
    pd.reset_option('display.max_rows') # Optional: reset to default