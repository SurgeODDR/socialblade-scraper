import os
import time
import json
import pandas as pd
import requests
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest, InternalServerError
from typing import List
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://keyvaultxscrapingoddr.vault.azure.net/", credential=credential)
secret = secret_client.get_secret("YT-Scraper-web-googleservicekey")
creds = json.loads(secret.value)

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
    "youtube": ["Twistzz", "nisqy9099", "UCqA5q4Qj0oFtsCXzAwzvJ5w", "caedrel", "kiittylol", "rushlol"],
    "instagram": ["twistzzcs", "jlekavicius", "rekkles", "nisqylol", "bwipolol", "cabochardlol", "jensenliquid", "caedrel", "yamatocannon1", "kiittwy", "rushlol", "mediccasts", "tifa_lol", "lizialol", "colomblbl", "karinak_lol"],
    "twitter": ["Twistzz", "jLcsgo_", "RekklesLoL", "Nisqy", "Bwipo", "CabochardLoL", "Caedrel", "Spicalol", "Jensen", "zyblol", "yamatomebdi", "RushLoL", "TIFA_LoL", "lizialol", "Colomblbl", "karinak_lol"],
    "tiktok": ["twistzzca", "jlcsgo", "lolrekkles", "zyblol", "caedrel", "colomblbl", "karinaklol"]
}

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

def rename_headers(headers: List[str]) -> dict:
    header_mapping = {
        "status.success": "Status Success",
        "data.statistics.total.media": "Total Media",
        "data.statistics.total.engagement_rate": "Total Engagement Rate",
        "data.statistics.average.likes": "Average Likes",
        "data.statistics.average.comments": "Average Comments",
        "data.statistics.growth.media.1": "Media Growth (1 Day)",
        "data.statistics.growth.media.3": "Media Growth (3 Days)",
        "data.statistics.growth.media.7": "Media Growth (7 Days)",
        "data.statistics.growth.media.14": "Media Growth (14 Days)",
        "data.statistics.growth.media.30": "Media Growth (30 Days)",
        "data.statistics.growth.media.60": "Media Growth (60 Days)",
        "data.statistics.growth.media.90": "Media Growth (90 Days)",
        "data.statistics.growth.media.180": "Media Growth (180 Days)",
        "data.statistics.growth.media.365": "Media Growth (365 Days)",
        "data.ranks.media": "Media Rank",
        "data.ranks.engagement_rate": "Engagement Rate Rank",
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
    new_header_mapping = {header: new_header for header, new_header in header_mapping.items() if header in headers and header not in ignore_columns}
    return new_header_mapping

@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    for platform, users in platforms_users.items():
        dfs = []  # Create an empty list for storing dataframes
        for user in users:
            response = requests.get(api_url.format(platform, user), headers={'CLIENT_ID': app.config['CLIENT_ID'], 'ACCESS_TOKEN': app.config['ACCESS_TOKEN']})
            data = response.json()
            df = pd.json_normalize(data)
            df.rename(columns=rename_headers(df.columns), inplace=True)  # Rename columns using new_header_mapping
            dfs.append(df)  # Append the DataFrame to the list
        platform_df = pd.concat(dfs, ignore_index=True)  # Concatenate all dataframes in the list
        csv_data = platform_df.to_csv(index=False)  # Convert the entire platform DataFrame to CSV
        blob_name = f'{platform}_{time.strftime("%Y%m%d-%H%M%S")}.csv'
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_data, blob_type="BlockBlob")
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True)
