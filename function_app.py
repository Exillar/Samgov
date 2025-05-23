import azure.functions as func
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import json
import os
from azure.storage.blob import BlobClient
import time
import pandas as pd
from io import BytesIO, StringIO
import csv

app = func.FunctionApp()

# Configuration
endpoint = "https://www.highergov.com/api-external/grant/"
api_key = os.getenv("HIGHERGOV_API_KEY")
max_workers = 2  # for memory/rate safety
blob_conn_str = os.getenv("AzureWebJobsStorage")
container_name = "landing"

def fetch_data(date_str, session, search_id):
    records = []
    params = {
        "api_key": api_key,
        "search_id": search_id,
        "last_modified_date": date_str,
        "page_size": 100
    }
    page = 1
    while True:
        try:
            params["page"] = page
            response = session.get(endpoint, params=params)
            if response.status_code != 200:
                break

            data = response.json()
            page_records = data.get("results", [])
            for r in page_records:
                record = {
                    "Award ID": r.get("award_id", ""),
                    "Recipient Name": r.get("awardee_key", {}).get("clean_name", ""),
                    "Recipient UEI": r.get("awardee_key", {}).get("uei", ""),
                    "Recipient CAGE": r.get("awardee_key", {}).get("cage_code", ""),
                    "Recipient Path": r.get("awardee_key", {}).get("path", ""),
                    "Parent Name": r.get("awardee_key_parent", {}).get("clean_name", ""),
                    "Parent UEI": r.get("awardee_key_parent", {}).get("uei", ""),
                    "Assistance Type Code": r.get("assistance_type_code", ""),
                    "Latest Transaction Key": r.get("latest_transaction_key", ""),
                    "Last Modified Date": r.get("last_modified_date", ""),
                    "Latest Action Date": r.get("latest_action_date", ""),
                    "Fiscal Year": r.get("latest_action_date_fiscal_year", ""),
                    "Start Date": r.get("period_of_performance_start_date", ""),
                    "End Date": r.get("period_of_performance_current_end_date", ""),
                    "Total Obligated Amount": r.get("total_obligated_amount", 0),
                    "Federal Obligation": r.get("federal_action_obligation", 0),
                    "Non-Federal Amount": r.get("non_federal_funding_amount", 0),
                    "Solicitation ID": r.get("solicitation_identifier", ""),
                    "ZIP": r.get("primary_place_of_performance_zip", ""),
                    "County": r.get("primary_place_of_performance_county_name", ""),
                    "City": r.get("primary_place_of_performance_city_name", ""),
                    "State Code": r.get("primary_place_of_performance_state_code", ""),
                    "State Name": r.get("primary_place_of_performance_state_name", ""),
                    "Country": r.get("primary_place_of_performance_country_name", ""),
                    "Description": r.get("award_description_original", ""),
                    "CFDA Number": r.get("grant_program", {}).get("cfda_program_number", ""),
                    "Program Title": r.get("grant_program", {}).get("program_title", ""),
                    "Popular Program Title": r.get("grant_program", {}).get("popular_program_title", ""),
                    "Awarding Agency": r.get("awarding_agency", {}).get("agency_name", ""),
                    "Awarding Agency Abbr.": r.get("awarding_agency", {}).get("agency_abbreviation", ""),
                    "Funding Agency": r.get("funding_agency", {}).get("agency_name", ""),
                    "Funding Agency Abbr.": r.get("funding_agency", {}).get("agency_abbreviation", "")
                    # Capture time will be added later
                }
                records.append(record)

            if len(page_records) < params["page_size"] or not data.get("next_page"):
                break

            page += 1
            time.sleep(0.3)

        except Exception as e:
            print(f"Error fetching {date_str} page {page}: {str(e)}")
            break

    return date_str, records

def convert_json_blob_to_parquet(keyword, month_key):
    try:
        # Construct blob paths
        json_blob_name = f"Staging/{keyword}/{keyword}{month_key}.json"
        parquet_blob_name = f"Bronze/{keyword}/{keyword}{month_key}.parquet"

        # Download JSON blob
        json_blob_client = BlobClient.from_connection_string(
            blob_conn_str, container_name=container_name, blob_name=json_blob_name
        )
        json_data = json_blob_client.download_blob().readall().decode("utf-8")

        # Convert JSON to DataFrame
        df = pd.read_json(StringIO(json_data))

        # Convert to Parquet in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        # Upload to bronze folder
        parquet_blob_client = BlobClient.from_connection_string(
            blob_conn_str, container_name=container_name, blob_name=parquet_blob_name
        )
        parquet_blob_client.upload_blob(parquet_buffer, overwrite=True)
        print(f"Converted and uploaded Parquet for {month_key}")

    except Exception as e:
        print(f"Error converting JSON to Parquet for {month_key}: {str(e)}")

def update_log_csv(keyword, search_id, start_date_str, end_date_str, total_records, capture_time):
    try:
        log_blob_name = f"Staging/log.csv"
        log_blob_client = BlobClient.from_connection_string(
            blob_conn_str, container_name=container_name, blob_name=log_blob_name
        )
        
        # Define column headers
        headers = ["Keyword", "SearchID", "StartDate", "EndDate", "TotalRecords", "CaptureTime"]
        
        # Create new log entry
        new_entry = {
            "Keyword": keyword,
            "SearchID": search_id,
            "StartDate": start_date_str,
            "EndDate": end_date_str,
            "TotalRecords": total_records,
            "CaptureTime": capture_time
        }
        
        try:
            # Try to download existing CSV
            existing_csv = log_blob_client.download_blob().readall().decode("utf-8")
            df = pd.read_csv(StringIO(existing_csv))
            
            # Check if entry with same key fields exists
            mask = (
                (df["Keyword"] == keyword) & 
                (df["SearchID"] == search_id) & 
                (df["StartDate"] == start_date_str) & 
                (df["EndDate"] == end_date_str)
            )
            
            if mask.any():
                # Update existing entry
                for col, val in new_entry.items():
                    df.loc[mask, col] = val
            else:
                # Append new entry
                df = pd.concat([df, pd.DataFrame([new_entry])], ignore_index=True)
                
        except:
            # If CSV doesn't exist, create new DataFrame
            df = pd.DataFrame([new_entry], columns=headers)
        
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload updated CSV
        log_blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"Updated log CSV with entry for {keyword}")
        
    except Exception as e:
        print(f"Error updating log CSV: {str(e)}")

@app.function_name(name="highergov")
@app.route(route="highergov", auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        keyword = req_body.get("keyword")
        start_date_str = req_body.get("start_date")
        end_date_str = req_body.get("end_date")
        search_id = req_body.get("search_id")

        if not keyword or not search_id:
            return func.HttpResponse("Missing required parameters: keyword and search_id", status_code=400)
        
        # Default date range if not specified
        if not start_date_str:
            start_date_str = "2024-01-01"
        if not end_date_str:
            end_date_str = "2024-12-31"

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        monthly_records = {}
        date_list = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d")
                     for x in range((end_date - start_date).days + 1)]

        # Get current capture time once for all records
        capture_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(lambda date: fetch_data(date, session, search_id), date_list)
                for date_str, result in results:
                    # Add capture time to each record
                    for record in result:
                        record["Capture Time"] = capture_time
                        
                    month_key = date_str[:7].replace("-", "_")
                    if month_key not in monthly_records:
                        monthly_records[month_key] = []
                    monthly_records[month_key].extend(result)
                    time.sleep(0.5)

        total_records = 0
        
        for month_key, records in monthly_records.items():
            if records:
                try:
                    # Upload JSON to Staging
                    json_blob_name = f"Staging/{keyword}/{keyword}{month_key}.json"
                    json_blob_client = BlobClient.from_connection_string(
                        blob_conn_str, container_name=container_name, blob_name=json_blob_name
                    )
                    json_blob_client.upload_blob(json.dumps(records), overwrite=True)
                    total_records += len(records)

                    # Convert and upload Parquet to Bronze
                    convert_json_blob_to_parquet(keyword, month_key)

                except Exception as e:
                    print(f"Upload/Conversion error for {month_key}: {str(e)}")
        
        # Update log CSV
        update_log_csv(keyword, search_id, start_date_str, end_date_str, total_records, capture_time)

        return func.HttpResponse(f"Saved {total_records} records", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Internal error: {str(e)}", status_code=500)