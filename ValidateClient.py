import requests
import csv
from datetime import datetime
import asyncio

now = datetime.now()
dateTimeNowStr = now.strftime('%Y%m%d%H%M%S')

### variables
baseUrl = ""
loginUrl = f"{baseUrl}/api/Token"
getMsaWithClientContractsUrl = f"{baseUrl}/api/Contracts/GetMsaWithClientContracts?clientShortCode="
migrationFilePath = "TestFiles/2024-March-21 ARExportPlusEngagedAndNonEngagedStaff - Sheet1.csv" # Add here the relative path of the source file
processedMigrationFilePath = "TestFiles/ProcessedFiles/" # This is the output folder
payload = {
    "username": "",
    "password": ""
}
accessToken = ""

async def login():
    loginResponse = requests.post(loginUrl, json=payload)

    if loginResponse.status_code == 200:
        responseData = loginResponse.json()

        global accessToken 
        accessToken = responseData['access_token']

        print("Login successful!")
    else:
        print("Login failed!")

async def getClientContract(clientShortCode = ""):
    # Set bearer token in authentication
    headers = {
        "Authorization": f"Bearer {accessToken}"
    }

    # Append short code as parameter
    url = getMsaWithClientContractsUrl + clientShortCode

    response = requests.get(url, headers = headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON
        clientContracts = response.json()
        
        # Validate if short code should be in client or staff level import file
        if len(clientContracts) == 0:
            print(f"Added {clientShortCode} in client level import file.")
            return 0
        else:
            print(f"Added {clientShortCode} in staff level import file.")
            return 1
    else:
        print("Failed to retrieve client contracts", response.status_code)

async def validateClients():
    clientLevelShortCodes = set()
    staffLevelShortCodes = set()

    await login()
    
    clientShortCodes = await processFile()

    for shortCode in clientShortCodes:
        action = await getClientContract(shortCode)

        if action == 0: #0 is equal to remove, 1 is to retain
            staffLevelShortCodes.add(shortCode)
        else:
            clientLevelShortCodes.add(shortCode)

    await generateClientLevelImportTemplate(clientLevelShortCodes)
    await generateStaffLevelImportTemplate(staffLevelShortCodes)
    
async def processFile():
    # Initialize an empty set to keep track of unique rows
    unique_shortCodes = set()

    # Open the CSV file
    with open(migrationFilePath, newline='') as csvfile:

        # DictReader uses the first row as column headers by default
        csv_reader = csv.DictReader(csvfile)

        # Iterate over each row 
        for data in csv_reader:
            # Access specific columns by their header names
            clientShortCode = data['SC-ShortCode']

            if clientShortCode not in unique_shortCodes:
                unique_shortCodes.add(clientShortCode)
    
    return unique_shortCodes

async def generateStaffLevelImportTemplate(shortCodes):
    rows_to_keep = []

    # Open the CSV file
    with open(migrationFilePath, newline='') as csvfile, open(processedMigrationFilePath + f"StaffLevelImportFile-{dateTimeNowStr}.csv", 'x', newline='', encoding='utf-8') as outfile:

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

            if clientShortCode not in shortCodes:
                rows_to_keep.append(data)

        csv_writer.writerows(rows_to_keep)

async def generateClientLevelImportTemplate(shortCodes):
    rows_to_keep = []

    # Open the CSV file
    with open(migrationFilePath, newline='') as csvfile, open(processedMigrationFilePath + f"ClientLevelImportFile-{dateTimeNowStr}.csv", 'x', newline='', encoding='utf-8') as outfile:

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

            if clientShortCode not in shortCodes:
                rows_to_keep.append(data)

        csv_writer.writerows(rows_to_keep)
    
##Init
#asyncio.run(login()) ## Do test login
asyncio.run(validateClients())