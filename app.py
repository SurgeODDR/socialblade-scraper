from flask import Flask
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

def extract_instagram_data(item):
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
    row.extend(growth_followers)
    row.extend(growth_media)
    return row, [
        'Username', 'Display Name', 'Total Media', 'Total Followers', 'Total Following', 'Total Engagement Rate',
        'Average Likes', 'Average Comments',
        '1 Day Followers Growth', '3 Days Followers Growth', '7 Days Followers Growth', '14 Days Followers Growth', '30 Days Followers Growth',
        '60 Days Followers Growth', '90 Days Followers Growth', '180 Days Followers Growth', '365 Days Followers Growth',
        '1 Day Media Growth', '3 Days Media Growth', '7 Days Media Growth', '14 Days Media Growth', '30 Days Media Growth',
        '60 Days Media Growth', '90 Days Media Growth', '180 Days Media Growth', '365 Days Media Growth'
    ]

def extract_tiktok_data(item):
    row = [
        item['data']['id']['username'],
        item['data']['id']['display_name'],
        item['data']['statistics']['total']['followers'],
        item['data']['statistics']['total']['following'],
        item['data']['statistics']['total']['uploads'],
        item['data']['statistics']['total']['likes'],
    ]
    return row, ['Username', 'Display Name', 'Total Followers', 'Total Following', 'Total Uploads', 'Total Likes']

def extract_twitch_data(item):
    row = [
        item['data']['id']['username'],
        item['data']['id']['display_name'],
        item['data']['statistics']['total']['followers'],
        item['data']['statistics']['total']['views'],
    ]
    growth_followers = [item['data']['statistics']['growth']['followers'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
    row.extend(growth_followers)
    return row, [
        'Username', 'Display Name', 'Total Followers', 'Total Views',
        '1 Day Followers Growth', '3 Days Followers Growth', '7 Days Followers Growth', '14 Days Followers Growth', '30 Days Followers Growth',
        '60 Days Followers Growth', '90 Days Followers Growth', '180 Days Followers Growth', '365 Days Followers Growth'
    ]

def extract_youtube_data(item):
    row = [
        item['data']['id'].get('handle', None),
        item['data']['id']['display_name'],
        item['data']['statistics']['total']['uploads'],
        item['data']['statistics']['total']['subscribers'],
        item['data']['statistics']['total']['views'],
    ]
    growth_followers = [item['data']['statistics']['growth']['subs'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
    growth_media = [item['data']['statistics']['growth']['vidviews'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]
    row.extend(growth_followers)
    row.extend(growth_media)
    return row, [
        'Handle', 'Display Name', 'Total Uploads', 'Total Subscribers', 'Total Views',
        '1 Day Subscribers Growth', '3 Days Subscribers Growth', '7 Days Subscribers Growth', '14 Days Subscribers Growth', '30 Days Subscribers Growth',
        '60 Days Subscribers Growth', '90 Days Subscribers Growth', '180 Days Subscribers Growth', '365 Days Subscribers Growth',
        '1 Day Views Growth', '3 Days Views Growth', '7 Days Views Growth', '14 Days Views Growth', '30 Days Views Growth',
        '60 Days Views Growth', '90 Days Views Growth', '180 Days Views Growth', '365 Days Views Growth'
    ]

def extract_twitter_data(item):
    # Assuming a similar structure to Instagram for the extraction (modify as needed)
    row = extract_instagram_data(item)[0]
    return row, [
        'Username', 'Display Name', 'Total Media', 'Total Followers', 'Total Following',
        # Add other headers as required based on the JSON structure
    ]

EXTRACTION_FUNCTIONS = {
    'instagram_talent_data.json': extract_instagram_data,
    'tiktok_talent_data.json': extract_tiktok_data,
    'twitter_talent_data.json': extract_twitter_data,
    'twitch_talent_data.json': extract_twitch_data,
    'youtube_talent_data.json': extract_youtube_data,
}

@app.route('/run')
def run_script():
    key_vault_uri = "https://keyvaultxscrapingoddr.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)
    google_api_credentials = client.get_secret("YT-Scraper-web-googleservicekey")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(google_api_credentials.value))
    gc = gspread.authorize(creds)
    spreadsheet = gc.open('twitch_data')

    storage_account_url = "https://scrapingstoragex.blob.core.windows.net/"
    container_name = "scrapingstoragecontainer"
    blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
    json_files = list(EXTRACTION_FUNCTIONS.keys())

    for json_file in json_files:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=json_file)
        data = json.loads(blob_client.download_blob().readall().decode('utf-8'))

        extracted_data = []
        headers = []
        extraction_function = EXTRACTION_FUNCTIONS[json_file]
        for item in data:
            row, headers = extraction_function(item)
            extracted_data.append(row)

        worksheet = spreadsheet.add_worksheet(title=json_file.replace(".json", ""), rows="1", cols="1")
        worksheet.insert_row(headers, index=1)
        for row in extracted_data:
            worksheet.append_row(row)

    return "Script executed"

if __name__ == '__main__':
    app.run(debug=True)
