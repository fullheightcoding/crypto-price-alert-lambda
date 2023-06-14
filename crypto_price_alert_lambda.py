import logging
import boto3
import urllib.request
import json
import asyncio

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Read API URLs from JSON file
with open('api_urls.json') as f:
    api_urls_data = json.load(f)

# Constants for API endpoints
API_URLS = api_urls_data

def get_parameter(parameter_name):
    try:
        ssm_client = boto3.client('ssm')
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Failed to retrieve parameter {parameter_name}: {str(e)}")
        raise

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

        # Retrieve SNS topic ARN from AWS Systems Manager Parameter Store
        sns_topic_arn = get_parameter('/crypto_price_alert/sns_topic_arn')

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
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

    return 'Price check complete.'

def lambda_handler(event, context):
    threshold_coin = event.get('threshold_coin')
    threshold_price = event.get('threshold_price')
    threshold_direction = event.get('threshold_direction')

    if threshold_price and threshold_coin and threshold_direction:
        try:
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(main(threshold_coin, threshold_price, threshold_direction))
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
