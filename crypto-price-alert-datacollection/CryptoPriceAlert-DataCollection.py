import logging
import boto3
import urllib.request
import json
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Read API URLs from JSON file
with open('api_urls.json') as f:
    api_urls_data = json.load(f)

# Constants for API endpoints
API_URLS = api_urls_data

# Specify the DynamoDB table name
dynamodb_table_name = 'CryptoPrices'

def get_parameter(parameter_name):
    try:
        ssm_client = boto3.client('ssm')
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Failed to retrieve parameter {parameter_name}: {str(e)}")
        raise

# Call get_parameter() outside of lambda_handler so it can be reused by lambda context on mult invokes
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

async def main(threshold_coin, threshold_price, threshold_direction, threshold_denomination):
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
        # if (threshold_direction == 'less_than' and price[threshold_coin]['usd'] < threshold_price) or \
        #    (threshold_direction == 'greater_than' and price[threshold_coin]['usd'] > threshold_price):
        #     if sns_topic_arn:
        #         message = f"{threshold_coin.capitalize()} price has {threshold_direction} {threshold_price} USD. Current price: {price[threshold_coin]['usd']} USD."
        #         sns_client.publish(TopicArn=sns_topic_arn, Message=message)
        #         logger.info(f"{threshold_coin.capitalize()} price notification sent.")
        #     else:
        #         logger.warning("SNS topic ARN not available.")
        if (threshold_direction == 'less_than' and price[threshold_coin][threshold_denomination] < threshold_price) or \
           (threshold_direction == 'greater_than' and price[threshold_coin][threshold_denomination] > threshold_price):
            if sns_topic_arn:
                message = f"{threshold_coin.capitalize()} price has {threshold_direction} {threshold_price} {threshold_denomination}. Current price: {price[threshold_coin][threshold_denomination]} {threshold_denomination}."
                sns_client.publish(TopicArn=sns_topic_arn, Message=message)
                logger.info(f"{threshold_coin.capitalize()} price notification sent.")
            else:
                logger.warning("SNS topic ARN not available.")

        # Write the data to DynamoDB
        dynamodb_client = boto3.client('dynamodb')
        current_date = datetime.now().strftime('%Y-%m-%d')
        #timestamp = str(int(datetime.now().timestamp()))

        # Calculate the TTL duration in seconds (30 days = 30 * 24 * 60 * 60 seconds)
        ttl_duration = 30 * 24 * 60 * 60
        # Calculate the expiration time
        current_time = datetime.utcnow()
        expiration_time = current_time + timedelta(seconds=ttl_duration)
        # Convert expiration time to UNIX timestamp
        expiration_timestamp = str(int(expiration_time.timestamp()))

        actual_price_in_denominated_value = 0
        if price[threshold_coin][threshold_coin] == 'usd':
            actual_price_in_denominated_value = str(price[threshold_coin]['usd'])
        elif price[threshold_coin][threshold_coin] == 'btc':
            actual_price_in_denominated_value = str(price[threshold_coin]['btc'])

        item = {
            'CryptoSymbol': {'S': threshold_coin},
            'Date': {'S': current_date},
            # 'Price': {'N': str(price[threshold_coin]['usd'])},
            'Price': {'N': actual_price_in_denominated_value},
            'Denomination': {'S': price[threshold_denomination][threshold_denomination]},
            'DateTTL': {'S': expiration_timestamp}
        }

        response = dynamodb_client.put_item(
            TableName=dynamodb_table_name,
            Item=item
        )
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

    return 'Price check complete.'

def lambda_handler(event, context):
    threshold_coin = event.get('threshold_coin')
    threshold_price = event.get('threshold_price')
    threshold_direction = event.get('threshold_direction')
    threshold_denomination = event.get('threshold_denomination')

    if threshold_price and threshold_coin and threshold_direction:
        try:
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(main(threshold_coin, threshold_price, threshold_direction, threshold_denomination))
            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        except Exception as e:
            logger.error(f"Lambda handler error: {str(e)}")
            return {
                'statusCode': 500,
                'body': 'Internal server error.'
            }
    else:
        return {
            'statusCode': 400,
            'body': 'Threshold coin, threshold price, or threshold direction not provided.'
        }
