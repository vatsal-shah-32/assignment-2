# Import necessary libraries
import pandas as pd
from datetime import datetime, timedelta
import logging

# Start logging with custom formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

logging.info(f"Now first import file in csv to here with accurate file path.")
try:
    df = pd.read_csv("C:/Users/HP/Desktop/Wolke K8/input.csv")
    logging.info("File has been successfully loaded.")
except FileNotFoundError:
    logging.error("File not found. Please check the file path.")
    df = None
except Exception as e:
    logging.error(f"An error occurred while loading the file: {e}")
    df = None


logging.info(f"Now check some basic details of data in this file.")
print(f"~> Rows and Columns in Data : {df.shape[0]} Rows & {df.shape[1]} Columns in this file")
print("~> Data's Informations : ")
df.info()
print(f"~> Checking Null Value in Data : {df.isnull().sum().sum()} ")

# delete some columns
logging.info(f"as per the information to give me, I delete some columns...")
df.drop(['Subscription Name', 'Rate'], axis=1, inplace=True)
logging.info(f"Columns deleted are successfully...")
print(f"~> Now the names of the remaining columns are :")
print(df.columns)

# create 1st new columns
logging.info(f"Next create 1st new column as     ")
logging.debug(f"Process 1st column are start...")
def find_word_for_col1(word1):
    text1 = word1.split(" - ")
    if "instance Usage" in text1[0]:
        return "Instance Usage"
    elif "stack Usage" in text1[0]:
        return "Stack Usage"
    else:
        return "Not Found"
df['resource/service'] = df['Detailed Description'].apply(find_word_for_col1)
logging.info(f"1st new column is generating of complete process are done")
print(df['resource/service'].head(5))

# create 2nd new column
logging.info(f"Next create 2nd new column as [resource/usage_family] ")
logging.debug(f"Process for 2nd column are start...")

def find_word_for_col2(word2):
    text2 = word2.split(" - ")
    if "traces" in text2[1]:
        return "Traces"
    elif "logs" in text2[1]:
        return "Log"
    elif "prom" in text2[1]:
        return "Prometheus"
    elif "Total Unique Users" in text2[2]:
        return "Unique User Access"
    elif "test executions" in text2[2]:
        return "Test Execution"
    else:
        return "Unknown"
df['resource/usage_family'] = df['Detailed Description'].apply(find_word_for_col2)
logging.info(f"2nd new column is generating of complete process are done")
print(df['resource/usage_family'].head(5))

# create 3rd new column
logging.info(f"Next create 3rd new column as [usage/units] ")
logging.debug(f"Process for 3rd column are start...")

def find_word_for_col3(word3):
    text3 = word3.split(" - ")
    if "traces" in text3[3]:
        return "traces"
    elif "GB-mo" in text3[2]:
        return "GB-mo"
    elif "GB" in text3[2]:
        return "GB"
    elif "Users" in text3[2]:
        return "User"
    elif "test executions" in text3[2]:
        return "test executions"
    else:
        return "Unknown"
df['usage/units'] = df['Detailed Description'].apply(find_word_for_col3)
logging.info(f"3rd new column is generating of complete process are done")
print(df['usage/units'].head(5))

# create 4th new column
logging.info(f"Next create 4th new column as lineitem/type2")
logging.debug(f"Process for 4th column are start...")

d = []

for _, i in df.iterrows():
    if i["Amount"] < 0:
        d.append("Discount")
    else:
        d.append("Usage")
df['lineitem/type2'] = d

logging.info(f"4th new column is generating of complete process are done")
print(df['lineitem/type2'].head(5))

# Convert 'service start date' and 'service end date' columns to datetime objects
df['Service Start Date'] = pd.to_datetime(df['Service Start Date'])
df['Service End Date'] = pd.to_datetime(df['Service End Date'])
logging.info(f"coverting columns like service start date and service end date datatype: object to date and time ")

# change date formate 
logging.debug(f"change dateformate")
df['Service Start Date'] = df['Service Start Date'].dt.strftime('%m-%d-%Y')
df['Service End Date'] = df['Service End Date'].dt.strftime('%m-%d-%Y')
logging.info(f"dateformate is successfully changed...")

# Generate hourly intervals
logging.info(f"generate hourly intervals wise rows process are start...")
def generate_hourly_intervals(start_date, end_date):
    if len(start_date) == 10:
        start_date += 'T00:00:00'
    if len(end_date) == 10:
        end_date += 'T23:59:59'
        start = datetime.strptime(start_date, '%m-%d-%YT%H:%M:%S')
        end = datetime.strptime(end_date, '%m-%d-%YT%H:%M:%S')
        hourly_intervals = []
        current_time = start
        while current_time < end:
            next_time = current_time + timedelta(hours=1) - timedelta(seconds=1)
            hourly_intervals.append((current_time.strftime('%m-%d-%YT%H:%M:%S'), next_time.strftime('%m-%d-%YT%H:%M:%S')))
            current_time = next_time + timedelta(seconds=1)
        return hourly_intervals

new_rows = []
for _, row in df.iterrows():
    start_date = row['Service Start Date']
    end_date = row['Service End Date']
    hourly_intervals = generate_hourly_intervals(start_date, end_date)
    for interval in hourly_intervals:
        new_rows.append({
            'lineitem/type': row['Subscription Item'],
            'lineitem/description': row['Detailed Description'],
            'time/usage_start': interval[0],
            'time/usage_end': interval[1],
            'cost/discounted_amortized_cost': row['Amount'] / 744,
            'resource/service': row['resource/service'],
            'resource/usage_family': row['resource/usage_family'],
            'usage/units': row['usage/units'],
            'lineitem/type2': row['lineitem/type2']
        })
logging.info(f"generate hourly intervals wise rows are successfully...")

logging.info(f"new_df is generating start...")
new_df = pd.DataFrame(new_rows)
new_df['cost/discounted_amortized_cost'] = new_df['cost/discounted_amortized_cost'].round(4)
logging.info(f"amount coverting process by (hourly) time wise are complete successfully...")

logging.info(f"new_df is ready...")
print(new_df.head())

new_df.to_csv("final_output.csv", index=False)
logging.info(f"final_output.csv file is download complete...")
