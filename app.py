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

def rename_headers(headers: List[str]) -> List[str]:
    # Mapping of original header to new header
    header_mapping = {
        "status.success": "Status Success",
        "status.status": "Status",
        "info.access.seconds_to_expire": "Access Expiration Time (Seconds)",
        "info.access.expires_at": "Access Expiration Time",
        "info.credits.available": "Available Credits",
        "info.credits.premium-credits": "Available Premium Credits",
        "data.id.id": "ID",
        "data.id.username": "Username",
        "data.id.cusername": "Custom Username",
        "data.id.handle": "Handle",
        "data.id.display_name": "Display Name",
        "data.general.created_at": "Account Creation Date",
        "data.general.channel_type": "Channel Type",
        "data.general.geo.country_code": "Country Code",
        "data.general.geo.country": "Country",
        "data.general.branding.avatar": "Avatar URL",
        "data.general.branding.banner": "Banner URL",
        "data.general.branding.website": "Website",
        "data.general.branding.social.facebook": "Facebook Page",
        "data.general.branding.social.twitter": "Twitter Handle",
        "data.general.branding.social.twitch": "Twitch Handle",
        "data.general.branding.social.instagram": "Instagram Handle",
        "data.general.branding.social.linkedin": "LinkedIn Profile",
        "data.general.branding.social.discord": "Discord Server",
        "data.general.branding.social.tiktok": "TikTok Handle",
        "data.statistics.total.uploads": "Total Uploads",
        "data.statistics.total.subscribers": "Total Subscribers",
        "data.statistics.total.views": "Total Views",
        "data.statistics.growth.subs.1": "Subscribers Growth (1 day)",
        "data.statistics.growth.subs.3": "Subscribers Growth (3 days)",
        "data.statistics.growth.subs.7": "Subscribers Growth (7 days)",
        "data.statistics.growth.subs.14": "Subscribers Growth (14 days)",
        "data.statistics.growth.subs.30": "Subscribers Growth (30 days)",
        "data.statistics.growth.subs.60": "Subscribers Growth (60 days)",
        "data.statistics.growth.subs.90": "Subscribers Growth (90 days)",
        "data.statistics.growth.subs.180": "Subscribers Growth (180 days)",
        "data.statistics.growth.subs.365": "Subscribers Growth (365 days)",
        "data.statistics.growth.vidviews.1": "Views Growth (1 day)",
        "data.statistics.growth.vidviews.3": "Views Growth (3 days)",
        "data.statistics.growth.vidviews.7": "Views Growth (7 days)",
        "data.statistics.growth.vidviews.14": "Views Growth (14 days)",
        "data.statistics.growth.vidviews.30": "Views Growth (30 days)",
        "data.statistics.growth.vidviews.60": "Views Growth (60 days)",
        "data.statistics.growth.vidviews.90": "Views Growth (90 days)",
        "data.statistics.growth.vidviews.180": "Views Growth (180 days)",
        "data.statistics.growth.vidviews.365": "Views Growth (365 days)",
        "data.misc.grade.color": "Grade Color",
        "data.misc.grade.grade": "Grade",
        "data.misc.sb_verified": "Verified on Social Blade",
        "data.misc.made_for_kids": "Made for Kids",
        "data.misc.tags": "Tags",
        "data.ranks.sbrank": "Social Blade Rank",
        "data.ranks.subscribers": "Subscribers Rank",
        "data.ranks.views": "Views Rank",
        "data.ranks.country": "Country Rank",
        "data.ranks.channel_type": "Channel Type Rank",
        "data.badges": "Badges",
        "data.statistics.total.followers": "Total Followers",
        "data.statistics.total.following": "Total Following",
        "data.statistics.total.tweets": "Total Tweets",
        "data.statistics.growth.followers.1": "Follower Growth (1 day)",
        "data.statistics.growth.followers.3": "Follower Growth (3 days)",
        "data.statistics.growth.followers.7": "Follower Growth (7 days)",
        "data.statistics.growth.followers.14": "Follower Growth (14 days)",
        "data.statistics.growth.followers.30": "Follower Growth (30 days)",
        "data.statistics.growth.followers.60": "Follower Growth (60 days)",
        "data.statistics.growth.followers.90": "Follower Growth (90 days)",
        "data.statistics.growth.followers.180": "Follower Growth (180 days)",
        "data.statistics.growth.followers.365": "Follower Growth (365 days)",
        "data.statistics.growth.tweets.1": "Tweet Growth (1 day)",
        "data.statistics.growth.tweets.3": "Tweet Growth (3 days)",
        "data.statistics.growth.tweets.7": "Tweet Growth (7 days)",
        "data.statistics.growth.tweets.14": "Tweet Growth (14 days)",
        "data.statistics.growth.tweets.30": "Tweet Growth (30 days)",
        "data.statistics.growth.tweets.60": "Tweet Growth (60 days)",
        "data.statistics.growth.tweets.90": "Tweet Growth (90 days)",
        "data.statistics.growth.tweets.180": "Tweet Growth (180 days)",
        "data.statistics.growth.tweets.365": "Tweet Growth (365 days)",
        "data.misc.recent.game": "Recent Game",
        "data.misc.recent.status": "Recent Status",
        "data.misc.mature_warning": "Mature Warning",
        "data.ranks.followers": "Followers Rank",
        "data.ranks.following": "Following Rank",
        "data.ranks.uploads": "Uploads Rank",
        "data.ranks.likes": "Likes Rank",
        "data.daily": "Daily Data",
        "data.statistics.total.likes": "Total Likes"
    }

    # Replace the original headers with the new ones
    new_headers = [header_mapping.get(header, header) for header in headers if header not in ignore_columns]

    return new_headers

ignore_columns = [
    "status.success",
    "data.general.media.recent",
    "data.general.geo.broadcaster_language",
    "data.general.geo.location",
    "data.misc.twitter_verified",
    "data.ranks.tweets",
    "status.status",
    "info.access.seconds_to_expire",
    "info.access.expires_at",
    "info.credits.available",
    "info.credits.premium-credits",
    "data.general.created_at",
    "data.general.channel_type",
    "data.general.geo.country_code",
    "data.general.branding.banner",
    "data.general.branding.website",
    "data.general.branding.social.facebook",
    "data.general.branding.social.twitter",
    "data.general.branding.social.twitch",
    "data.general.branding.social.instagram",
    "data.general.branding.social.linkedin",
    "data.general.branding.social.discord",
    "data.general.branding.social.tiktok",
    "data.misc.grade.color",
    "data.misc.grade.grade",
    "data.misc.sb_verified",
    "data.misc.made_for_kids",
    "data.misc.tags",
    "data.ranks.sbrank",
    "data.ranks.subscribers",
    "data.ranks.views",
    "data.ranks.country",
    "data.ranks.channel_type",
    "data.badges",
    "data.daily",
    "data.misc.grade.color",
    "data.misc.grade.grade",
    "data.misc.sb_verified",
    "data.ranks.sbrank",
    "data.ranks.followers",
    "data.ranks.following",
    "data.ranks.uploads",
    "data.ranks.likes",
    "status.success",
    "status.status",
    "info.access.seconds_to_expire",
    "info.access.expires_at",
    "info.credits.available",
    "info.credits.premium-credits",
    "data.general.branding.avatar",
    "data.id.id",
]

# Initialize a client to interact with Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
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