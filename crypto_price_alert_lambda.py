import logging
import boto3
import urllib.request
import json
import asyncio
from datetime import date

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Read API URLs from JSON file
with open('api_urls.json') as f:
    api_urls_data = json.load(f)

# Constants for API endpoints
API_URLS = api_urls_data

# Retrieve SNS topic ARN from AWS Systems Manager Parameter Store
sns_topic_arn = get_parameter('/crypto_price_alert/sns_topic_arn')

# DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = 'CryptoPrices'
table = dynamodb.Table(table_name)

async def fetch_price(api_url):
    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())
        return data
    except Exception as e:
        logger.error(f"Failed to fetch price from API: {str(e)}")
        raise

async def main(threshold_coin, threshold_price, threshold_direction):
    api_url = API_URLS.get(threshold_coin)
    if api_url is None:
        logger.error('Invalid threshold coin.')
        return 'Invalid threshold coin.'

    try:
        tasks = [fetch_price(api_url)]
        results = await asyncio.gather(*tasks)
        price = results[0]

        # SNS client
        sns_client = boto3.client('sns')

        # Log the current price
        logger.info(f"{threshold_coin.capitalize()} current price: {price[threshold_coin]['usd']}")

        # Check if the price meets the threshold condition and send an email notification
        if (threshold_direction == 'less_than' and price[threshold_coin]['usd'] < threshold_price) or \
           (threshold_direction == 'greater_than' and price[threshold_coin]['usd'] > threshold_price):
            if sns_topic_arn:
                message = f"{threshold_coin.capitalize()} price has {threshold_direction} {threshold_price} USD. Current price: {price[threshold_coin]['usd']} USD."
                sns_client.publish(TopicArn=sns_topic_arn, Message=message)
                logger.info(f"{threshold_coin.capitalize()} price notification sent.")
            else:
                logger.warning("SNS topic ARN not available.")
        
        # Store the CryptoSymbol, Date, and Price in DynamoDB
        item = {
            'CryptoSymbol': threshold_coin,
            'Date': str(date.today()),  # Modify this based on how you want to represent the date
            'Price': price[threshold_coin]['usd']
        }
        table.put_item(Item=item)
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

    return 'Price check complete.'

# The lambda_handler function remains the same
# ...
