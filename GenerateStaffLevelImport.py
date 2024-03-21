import csv
from datetime import datetime
import asyncio

now = datetime.now()
dateTimeNowStr = now.strftime('%Y%m%d%H%M%S')

processedTevisFileStaff = "TestFiles/ProcessedFiles/StaffLevelImportFile-20240321131247.csv" # This is the output folder
postDataStaffSummary = "TestFiles/ProcessedFiles/Summary Discrepancy Feburary(staff level).csv" # This is the output folder
processedMigrationFilePath = "TestFiles/ProcessedFiles/" # This is the output folder

async def processPostDataStaffSummary():
    staffIds = set()

    # Open the CSV file
    with open(postDataStaffSummary, newline='', encoding='utf-8') as csvfile:

        # DictReader uses the first row as column headers by default
        csv_reader = csv.DictReader(csvfile)

        # Iterate over each row 
        for data in csv_reader:
            # Access specific columns by their header names
            clientShortCode = data['Shortcode']
            EngagementId = data['Enagement ID']

            staffId = f"{clientShortCode}|{EngagementId.replace(" ", "")}"
            
            staffIds.add(staffId)

    await generateStaffLevelImportTemplate(staffIds)

async def generateStaffLevelImportTemplate(staffIds):
    rows_to_keep = []

    # Open the CSV file
    with open(processedTevisFileStaff, newline='', encoding='utf-8') as csvfile, open(processedMigrationFilePath + f"StaffSummaryBaseFromPostData-{dateTimeNowStr}.csv", 'x', newline='', encoding='utf-8') as outfile:

        # DictReader uses the first row as column headers by default
        csv_reader = csv.DictReader(csvfile)
        # Create a csv.writer object
        csv_writer = csv.DictWriter(outfile, fieldnames=csv_reader.fieldnames)

        # Write the headers to the output file
        csv_writer.writeheader()

        # Iterate over each row 
        for data in csv_reader:
            # Access specific columns by their header names
            clientShortCode = data['SC-ShortCode']
            EngagementId = data['EngagementId']

            staffId = f"{clientShortCode}|{EngagementId.replace(" ", "")}"

            if staffId in staffIds:
                rows_to_keep.append(data)

        csv_writer.writerows(rows_to_keep)
        print(len(rows_to_keep))

asyncio.run(processPostDataStaffSummary())