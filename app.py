import os
import json
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pandas import json_normalize
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest, InternalServerError
from typing import List

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Get a credential to authenticate to the Key Vault
credential = DefaultAzureCredential()

# Create a secret client using the credential
secret_client = SecretClient(vault_url="https://keyvaultxscrapingoddr.vault.azure.net/", credential=credential)

# Retrieve the secret
secret = secret_client.get_secret("YT-Scraper-web-googleservicekey")

# Use the secret value as the Google service account key
creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(secret.value))

app = Flask(__name__)

# Load configurations from environment variables
app.config['CLIENT_ID'] = os.getenv('CLIENT_ID')
app.config['ACCESS_TOKEN'] = os.getenv('ACCESS_TOKEN')
api_url = "https://matrix.sbapis.com/b/{}/statistics"

# Unchanged code from the original script
platforms_users = {
    "twitch": ["twistzztv", "jLcs2", "rekkles", "nisqyy", "bwipolol", "Cabochardlol", "caedrel", "spicalol", "jensen", "zyblol", "caedrel", "yamatocannon", "tenacityna", "kiittwy", "caedrel", "Rush", "mediccasts", "tifa_lol", "lizialol", "colomblbl", "karinak"],
    "youtube": ["Twistzz", "nisqy9099", "UCqA5q4Qj0oFtsCXzAwzvJ5w", "caedrel", "kiittylol", "rushlol"],
    "instagram": ["twistzzcs", "jlekavicius", "rekkles", "nisqylol", "bwipolol", "cabochardlol", "jensenliquid", "caedrel", "yamatocannon1", "kiittwy", "rushlol", "mediccasts", "tifa_lol", "lizialol", "colomblbl", "karinak_lol"],
    "twitter": ["Twistzz", "jLcsgo_", "RekklesLoL", "Nisqy", "Bwipo", "CabochardLoL", "Caedrel", "Spicalol", "Jensen", "zyblol", "yamatomebdi", "RushLoL", "TIFA_LoL", "lizialol", "Colomblbl", "karinak_lol"],
    "tiktok": ["twistzzca", "jlcsgo", "lolrekkles", "zyblol", "caedrel", "colomblbl", "karinaklol"]
}

# Initialize a client to interact with Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
client = gspread.authorize(creds)

# Open the spreadsheet
spreadsheet = client.open('twitch_data')

def get_socialblade_data(platform: str, username: str) -> dict:
    url = api_url.format(platform)
    headers = {
        "clientid": app.config['CLIENT_ID'],
        "token": app.config['ACCESS_TOKEN'],
        "query": username
    }
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        app.logger.error(f"Failed to get data from SocialBlade for {platform}/{username}. HTTP status code: {response.status_code}")
        return None

    try:
        data = response.json()
    except json.JSONDecodeError:
        app.logger.error(f"Failed to decode JSON from SocialBlade response for {platform}/{username}")
        return None

    return data

def append_data_to_sheet(sheet, headers: List[str], data: List):
    # Unchanged code from the original script
    pass

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    # Input validation
    platforms_users = request.json.get('platforms_users')
    if not platforms_users:
        raise BadRequest('Invalid input: platforms_users is required.')

    for platform, users in platforms_users.items():
        try:
            worksheet = spreadsheet.worksheet(platform)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=platform, rows="100", cols="20")

        for user in users:
            data = get_socialblade_data(platform, user)
            if data is None:
                continue

            flat_data = pd.json_normalize(data)
            headers = flat_data.columns.tolist()
            headers = [h for h in headers if h not in ignore_columns]
            flat_data = flat_data[headers]
            append_data_to_sheet(worksheet, headers, flat_data.values.tolist())

    return jsonify({'message': 'Data fetched successfully'}), 200

# Error handling
@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return jsonify({'message': str(e)}), 400

@app.errorhandler(InternalServerError)
def handle_internal_error(e):
    app.logger.error(str(e))
    return jsonify({'message': 'An internal error occurred.'}), 500

if __name__ == '__main__':
    app.run(debug=True)
