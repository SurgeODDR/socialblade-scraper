import os
import time
import json
import requests
from flask import Flask, request, jsonify
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()

# Set up BlobServiceClient
blob_service_client = BlobServiceClient(account_url=os.getenv('AZURE_STORAGE_ACCOUNT_URL'), credential=credential)
container_name = 'scrapingstoragecontainer'
container_client = blob_service_client.get_container_client(container_name)

app = Flask(__name__)
app.config['CLIENT_ID'] = 'cli_6aabef3d2b6503a79f79bd84'
app.config['ACCESS_TOKEN'] = '1ae13f00a2e97b3faf520cc4898d8c6e0d6abd325957f7e2a957c72eefc945d13dbd21df611ec1dfd36750f2c4479aa2c3fc5de2d02eb39c2f21b1ed9cdf4c6b'
api_url = "https://matrix.sbapis.com/b/{}/statistics"

platforms_users = {
    "twitch": ["twistzztv", "jLcs2", "rekkles", "nisqyy", "bwipolol", "Cabochardlol", "caedrel", "spicalol", "jensen", "zyblol", "caedrel", "yamatocannon", "tenacityna", "kiittwy", "caedrel", "Rush", "mediccasts", "tifa_lol", "lizialol", "colomblbl", "karinak"],
    "youtube": ["Twistzz", "@nisqy9099", "UCqA5q4Qj0oFtsCXzAwzvJ5w", "caedrel", "kiittylol", "@rushlol"],
    "instagram": ["twistzzcs", "jlekavicius", "rekkles", "nisqylol", "bwipolol", "cabochardlol", "jensenliquid", "caedrel", "yamatocannon1", "kiittwy", "rushlol", "mediccasts", "tifa_lol", "lizialol", "colomblbl", "karinak_lol"],
    "twitter": ["Twistzz", "jLcsgo_", "RekklesLoL", "Nisqy", "Bwipo", "CabochardLoL", "Caedrel", "Spicalol", "Jensen", "zyblol", "yamatomebdi", "RushLoL", "TIFA_LoL", "lizialol", "Colomblbl", "karinak_lol"],
    "tiktok": ["twistzzca", "jlcsgo", "lolrekkles", "zyblol", "caedrel", "colomblbl", "karinaklol"]
}

@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    for platform, users in platforms_users.items():
        if not platform:
            return jsonify({"status": "error", "message": "platform is empty"}), 400
        data_list = []
        for user in users:
            if not user:
                return jsonify({"status": "error", "message": "user is empty"}), 400
            response = requests.get(
                f"https://matrix.sbapis.com/b/{platform}/statistics",  # Updated API URL
                headers={
                    'query': user,  # Updated headers based on the API details
                    'history': 'default',
                    'clientid': app.config['CLIENT_ID'],
                    'token': app.config['ACCESS_TOKEN']
                }
            )

            if not response.ok:
                app.logger.error(f"Failed to fetch data for {platform}/{user}, status code: {response.status_code}")
                continue

            if 'application/json' not in response.headers['Content-Type']:
                app.logger.error(f"Unexpected content type for {platform}/{user}")
                continue

            try:
                data = response.json()
            except json.JSONDecodeError:
                app.logger.error(f"Failed to decode JSON for {platform}/{user}")
                continue

            if not data.get('status', {}).get('success'):  # Check if the API request was successful
                app.logger.error(f"API request unsuccessful for {platform}/{user}")
                continue

            data_list.append(data)

        json_data = json.dumps(data_list)
        blob_name = f'{platform}_talent_data.json'
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data, blob_type="BlockBlob")

    return jsonify({"status": "success"}), 200
