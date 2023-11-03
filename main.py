from fastapi import FastAPI,BackgroundTasks
from pymongo import MongoClient
from fastapi.responses import FileResponse
import pandas as pd
import pytz
import uuid
from datetime import datetime, timedelta


app = FastAPI()

# Connect to MongoDB
client = MongoClient("mongodb+srv://sarthak27998:w2RiKTRwyNaCCsun@cluster0.odnn1pg.mongodb.net/?retryWrites=true&w=majority")
db = client["mydatabase"]
collection1 = db["store_status"]
collection2 = db["store_hours"]
collection3 = db["store_timezone"]

# Read CSV file and store data into MongoDB
@app.get("/store_csv_data")
async def store_csv_data():
    df1 = pd.read_csv("store_status.csv")
    df2 = pd.read_csv("store_hours.csv")
    df3 = pd.read_csv("store_timezone.csv")



    # Fill missing values with 0 for day, 00:00 for start_time_local, and 23:59 for end_time_local
    df2 = df2.fillna({"day": 0, "start_time_local": "00:00", "end_time_local": "23:59"})

    # Fill missing values with America/Chicago for timezone_str
    df3 = df3.fillna({"timezone_str": "America/Chicago"})
    

    records1 = df1.to_dict(orient="records")
    records2 = df2.to_dict(orient="records")
    records3 = df3.to_dict(orient="records")
    collection1.insert_many(records1)
    collection2.insert_many(records2)
    collection3.insert_many(records3)
    return {"message": "Data stored successfully!"}

def calculate_uptime_downtime(store_id, current_timestamp):
    # Get the store_status data for the given store_id
    status_data = collection1.find({"store_id": store_id})

    # Get the store_hours data for the given store_id
    hours_data = collection2.find({"store_id": store_id})

    # Get the store_timezone data for the given store_id
    timezone_data = collection3.find_one({"store_id": store_id})

    # Initialize the variables for uptime and downtime
    uptime_last_hour = 0
    uptime_last_day = 0
    uptime_last_week = 0
    downtime_last_hour = 0
    downtime_last_day = 0
    downtime_last_week = 0

    # Convert the current timestamp to a datetime object
    current_timestamp = datetime.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S.%f UTC")

    # Convert the current timestamp to the store's timezone
    current_timestamp = current_timestamp.astimezone(pytz.timezone(timezone_data["timezone_str"]))

    # Loop through each day of the week
    for day in range(7):
        # Get the start and end time of the business hours for that day
        start_time = hours_data[day]["start_time_local"]
        end_time = hours_data[day]["end_time_local"]

        # Convert the start and end time to datetime objects
        start_time = datetime.strptime(start_time, "%H:%M:%S")
        end_time = datetime.strptime(end_time, "%H:%M:%S")

        # Create a datetime object for the start of that day
        day_start = current_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=day)

        # Create a datetime object for the end of that day
        day_end = day_start + timedelta(days=1)

        # Create a datetime object for the start of the business hours for that day
        business_start = day_start.replace(hour=start_time.hour, minute=start_time.minute)

        # Create a datetime object for the end of the business hours for that day
        business_end = day_start.replace(hour=end_time.hour, minute=end_time.minute)

        status_data = list(status_data)
        
        for i in range(len(status_data)):
            status_data[i]["timestamp_utc"] = datetime.strptime(status_data[i]["timestamp_utc"], "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=pytz.UTC)


        status_data_day = [s for s in status_data if day_start <= s["timestamp_utc"] <= day_end]  # Define status_data_day based on the current day


        prev_status = None
        prev_timestamp = None

        # Loop through each status data point for that day
        for s in status_data_day:
            # Get the current status and timestamp
            curr_status = s["status"]
            curr_timestamp = s["timestamp_utc"]

            # If this is the first status data point for that day, set the previous status and timestamp to be equal to it
            if prev_status is None and prev_timestamp is None:
                prev_status = curr_status
                prev_timestamp = curr_timestamp

            # If this is not the first status data point for that day, calculate the time difference between the current and previous timestamp
            else:
                time_diff = (curr_timestamp - prev_timestamp).total_seconds()

                # If both timestamps are within the business hours, add the time difference to either uptime or downtime based on the previous status
                if business_start <= prev_timestamp <= business_end and business_start <= curr_timestamp <= business_end:
                    if prev_status == "UP":
                        uptime_last_week += time_diff / 3600 # Convert seconds to hours
                        if day == 0: # If it is today
                            uptime_last_day += time_diff / 3600 
                            if curr_timestamp >= current_timestamp - timedelta(hours=1): # If it is within the last hour
                                uptime_last_hour += time_diff / 60 # Convert seconds to minutes 
                    elif prev_status == "DOWN":
                        downtime_last_week += time_diff / 3600 
                        if day == 0: 
                            downtime_last_day += time_diff / 3600 
                            if curr_timestamp >= current_timestamp - timedelta(hours=1): 
                                downtime_last_hour += time_diff / 60 

                # If only one of the timestamps is within the business hours, interpolate the time difference based on some sane logic
                elif (business_start <= prev_timestamp <= business_end) != (business_start <= curr_timestamp <= business_end):
                    # For simplicity, we assume that half of the time difference is within the business hours and half is outside
                    time_diff /= 2

                    # Add half of the time difference to either uptime or downtime based on the previous status
                    if prev_status == "UP":
                        uptime_last_week += time_diff / 3600 
                        if day == 0: 
                            uptime_last_day += time_diff / 3600 
                            if curr_timestamp >= current_timestamp - timedelta(hours=1): 
                                uptime_last_hour += time_diff / 60 
                    elif prev_status == "DOWN":
                        downtime_last_week += time_diff / 3600 
                        if day == 0: 
                            downtime_last_day += time_diff / 3600 
                            if curr_timestamp >= current_timestamp - timedelta(hours=1): 
                                downtime_last_hour += time_diff / 60 

                # Update the previous status and timestamp to be equal to the current ones
                prev_status = curr_status
                prev_timestamp = curr_timestamp

        # If there is no status data for that day, assume that the store is UP for the entire business hours
        if len(status_data_day) == 0:
            time_diff = (business_end - business_start).total_seconds()
            uptime_last_week += time_diff / 3600
            if day == 0:
                uptime_last_day += time_diff / 3600
                if business_start <= current_timestamp <= business_end:
                    uptime_last_hour += (current_timestamp - business_start).total_seconds() / 60

        # If there is only one status data point for that day, extrapolate the uptime or downtime based on that status
        elif len(status_data_day) == 1:
            # If the status is UP, assume that the store is UP from the start of the business hours to the current timestamp or the end of the business hours, whichever is earlier
            if curr_status == "UP":
                time_diff = min(current_timestamp, business_end) - max(business_start, curr_timestamp)
                time_diff = time_diff.total_seconds()
                uptime_last_week += time_diff / 3600
                if day == 0:
                    uptime_last_day += time_diff / 3600
                    if curr_timestamp <= current_timestamp <= business_end:
                        uptime_last_hour += (current_timestamp - curr_timestamp).total_seconds() / 60

            # If the status is DOWN, assume that the store is DOWN from the start of the business hours to the current timestamp or the end of the business hours, whichever is earlier
            elif curr_status == "DOWN":
                time_diff = min(current_timestamp, business_end) - max(business_start, curr_timestamp)
                time_diff = time_diff.total_seconds()
                downtime_last_week += time_diff / 3600
                if day == 0:
                    downtime_last_day += time_diff / 3600
                    if curr_timestamp <= current_timestamp <= business_end:
                        downtime_last_hour += (current_timestamp - curr_timestamp).total_seconds() / 60

    # Return a dictionary with the uptime and downtime values for each store
    return {
        "store_id": store_id,
        "uptime_last_hour": round(uptime_last_hour, 2),
        "uptime_last_day": round(uptime_last_day, 2),
        "uptime_last_week": round(uptime_last_week, 2),
        "downtime_last_hour": round(downtime_last_hour, 2),
        "downtime_last_day": round(downtime_last_day, 2),
        "downtime_last_week": round(downtime_last_week, 2)
    }
   
report_data = None
report_status = None

# Define a function that generates the report in the background
def generate_report(background_tasks: BackgroundTasks):
    # Set the global variables to access them inside the function
    global report_data
    global report_status

    # Set the report status to "Running"
    report_status = "Running"

    # Get all the unique store ids from the store_status collection
    store_ids = collection1.distinct("store_id")

    # Get the current timestamp as the max timestamp among all the observations in the first CSV
    current_timestamp = collection1.find_one(sort=[("timestamp_utc", -1)])["timestamp_utc"]

    # Initialize an empty list to store the report records
    report_records = []

    # Loop through each store id and calculate its uptime and downtime using the function defined earlier
    for store_id in store_ids:
        # Call the calculate_uptime_downtime function and append the result to the report records list
        report_records.append(calculate_uptime_downtime(store_id, current_timestamp))

    # Convert the report records list to a pandas dataframe
    report_df = pd.DataFrame(report_records)

    # Save the report dataframe as a CSV file
    report_df.to_csv("report.csv", index=False)

    # Set the report data to be equal to the report dataframe
    report_data = report_df

    # Set the report status to "Complete"
    report_status = "Complete"

# Create an API for triggering the report generation using the /trigger_report endpoint
@app.get("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    # Generate a random string as the report_id
    report_id = str(uuid.uuid4())

    # Run the generate_report function in the background
    background_tasks.add_task(generate_report, background_tasks)

    # Return the report_id as the output
    return {"report_id": report_id}

# Create an API for getting the report using the /get_report endpoint
@app.get("/get_report")
async def get_report(report_id: str):
    # Check if the report status is "Running"
    if report_status == "Running":
        # Return "Running" as the output
        return {"status": "Running"}
    
    # Check if the report status is "Complete"
    elif report_status == "Complete":
        # Return "Complete" along with the CSV file as a response
        return {"status": "Complete", "file": FileResponse("report.csv")}
    
    # If neither of the above cases are true, return an error message
    else:
        return {"error": "Invalid report_id"}
