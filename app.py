from flask import Flask
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

@app.route('/run')
def run_script():
    # Fetch the Google API credentials from Azure Key Vault
    key_vault_uri = "https://keyvaultxscrapingoddr.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)
    google_api_credentials = client.get_secret("YT-Scraper-web-googleservicekey")

    # Use creds to create a client to interact with the Google Sheets API
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(google_api_credentials.value))
    gc = gspread.authorize(creds)

    # Open the 'twitch_data' spreadsheet
    spreadsheet = gc.open('twitch_data')

    # Fetch JSON files from Azure Blob Storage
    storage_account_url = "https://scrapingstoragex.blob.core.windows.net/"
    container_name = "scrapingstoragecontainer"
    blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
    json_files = ["instagram_talent_data.json", "tiktok_talent_data.json", "twitter_talent_data.json", "twitch_talent_data.json", "youtube_talent_data.json"]

    for json_file in json_files:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=json_file)
        data = json.loads(blob_client.download_blob().readall().decode('utf-8'))

        # Extract the necessary data
        extracted_data = []
        for item in data:
            if 'media' in item['data']['statistics']['total']:
                row = [
                    item['data']['id']['username'],
                    item['data']['id']['display_name'],
                    item['data']['statistics']['total']['media'],
                    item['data']['statistics']['total']['followers'],
                    item['data']['statistics']['total']['following'],
                    item['data']['statistics']['total']['engagement_rate'],
                    item['data']['statistics']['average']['likes'],
                    item['data']['statistics']['average']['comments'],
                ]
                growth_followers = [item['data']['statistics']['growth']['followers'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
                growth_media = [item['data']['statistics']['growth']['media'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
            elif 'uploads' in item['data']['statistics']['total']:
                row = [
                    item['data']['id']['handle'],
                    item['data']['id']['display_name'],
                    item['data']['statistics']['total']['uploads'],
                    item['data']['statistics']['total']['subscribers'],
                    item['data']['statistics']['total']['views'],
                ]
                growth_followers = [item['data']['statistics']['growth']['subs'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
                growth_media = [item['data']['statistics']['growth']['vidviews'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
            elif 'followers' in item['data']['statistics']['total']:
                row = [
                    item['data']['id']['username'],
                    item['data']['id']['display_name'],
                    item['data']['statistics']['total']['followers'],
                    item['data']['statistics']['total']['views'],
                ]
                growth_followers = [item['data']['statistics']['growth']['followers'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
                growth_media = []
            elif json_file == "tiktok_talent_data.json":
                row = [
                    item['data']['id']['username'],
                    item['data']['id']['display_name'],
                    item['data']['statistics']['total']['followers'],
                    item['data']['statistics']['total']['following'],
                    item['data']['statistics']['total']['uploads'],
                    item['data']['statistics']['total']['likes'],
                ]
                growth_followers = []
                growth_media = []
            row.extend(growth_followers)
            row.extend(growth_media)
            extracted_data.append(row)

        # Append the data to the Google Spreadsheet
        worksheet = spreadsheet.add_worksheet(title=json_file.replace(".json", ""), rows="1", cols="1")
        headers = ['Username/Handle', 'Display Name', 'Total Media/Tweets/Uploads/Followers', 'Total Followers/Following/Subscribers/Views', 
                   'Total Following/Views', 'Total Engagement Rate',
                   'Average Likes/Comments', '1 Day Follower/Subscribers Growth', '3 Days Follower/Subscribers Growth', 
                   '7 Days Follower/Subscribers Growth', '14 Days Follower/Subscribers Growth', '30 Days Follower/Subscribers Growth', 
                   '60 Days Follower/Subscribers Growth', '90 Days Follower/Subscribers Growth', '180 Days Follower/Subscribers Growth', 
                   '365 Days Follower/Subscribers Growth', '1 Day Media/Tweets/Views Growth', '3 Days Media/Tweets/Views Growth', 
                   '7 Days Media/Tweets/Views Growth', '14 Days Media/Tweets/Views Growth', '30 Days Media/Tweets/Views Growth', 
                   '60 Days Media/Tweets/Views Growth', '90 Days Media/Tweets/Views Growth', '180 Days Media/Tweets/Views Growth', 
                   '365 Days Media/Tweets/Views Growth']
        worksheet.insert_row(headers, index=1)
        for row in extracted_data:
            worksheet.append_row(row)

    return "Script executed"

if __name__ == '__main__':
    app.run(debug=True)
