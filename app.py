from flask import Flask
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

def extract_instagram_data(item):
    total_stats = item['data']['statistics']['total']
    growth_stats = item['data']['statistics']['growth']
    return [
        item['data']['id']['username'],
        item['data']['id']['display_name'],
        total_stats.get('media', None),
        total_stats.get('followers', None),
        total_stats.get('following', None),
        total_stats.get('engagement_rate', None),
        item['data']['statistics']['average'].get('likes', None),
        item['data']['statistics']['average'].get('comments', None)
    ] + [growth_stats['followers'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]] + \
        [growth_stats['media'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]

def extract_twitter_data(item):
    total_stats = item['data']['statistics']['total']  # Assuming the structure is similar to Instagram
    growth_stats = item['data']['statistics']['growth']  # Assuming the structure is similar to Instagram
    return [
        item['data']['username'],  # Assuming the structure is similar to Instagram
        item['data']['display_name'],  # Assuming the structure is similar to Instagram
        total_stats.get('tweets', None),
        total_stats.get('followers', None),
        total_stats.get('following', None)
    ] + [growth_stats['followers'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]] + \
        [growth_stats['tweets'].get(str(i), None) for i in [1, 3, 7, 14, 30, 60, 90, 180, 365]]

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
