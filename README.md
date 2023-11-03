# Loop

Database Connection: It establishes a connection to a MongoDB database hosted on MongoDB Atlas.

CSV Data Import: The /store_csv_data endpoint reads data from three CSV files: "store_status.csv," "store_hours.csv," and "store_timezone.csv."
It fills missing values in the dataframes and stores the data into three separate collections in the MongoDB database: "store_status," "store_hours," and "store_timezone."

Uptime and Downtime Calculation:The calculate_uptime_downtime function calculates uptime and downtime for a specific store based on its status data.
It retrieves status data for a store, its business hours, and its timezone information from the MongoDB collections.
It then processes the status data, calculates uptime, and downtime for the last hour, last day, and last week.
The results are returned in a dictionary.

Report Generation:The /trigger_report endpoint generates a report in the background. It initiates the generate_report function in the background task.
The generate_report function:
Sets the report status to "Running."
Retrieves the unique store IDs.
Gets the current timestamp as the maximum timestamp from the "store_status" collection.
Calculates uptime and downtime for each store and stores the results in a Pandas DataFrame.
Saves the report as a CSV file.
Sets the report data and status to "Complete."

Report Retrieval:The /get_report endpoint allows users to retrieve the report.
If the report is still running (status is "Running"), it returns "Running."
If the report is complete (status is "Complete"), it returns "Complete" along with the generated CSV report file for download.
This code essentially provides a web service for storing CSV data into MongoDB, calculating uptime and downtime for stores, generating reports, and allowing users to retrieve the generated reports. Users can trigger report generation and check the status of report generation tasks.

Screenshot1

![image](https://github.com/sarthak37/Loop/assets/52873771/432801c6-b3a4-446c-bf76-cceee83fefe0)

Screenshot 2

![image](https://github.com/sarthak37/Loop/assets/52873771/3298be1c-08fb-4fde-a6ca-1337e09499ed)

Screenshot 3

![image](https://github.com/sarthak37/Loop/assets/52873771/41ac4ec3-9b81-4785-83dc-c4b551b3be54)

Screenshot 4

![image](https://github.com/sarthak37/Loop/assets/52873771/3a6d438f-c348-4450-9c48-8669852d14ac)



