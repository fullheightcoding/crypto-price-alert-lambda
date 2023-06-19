import boto3
import statistics
from collections import defaultdict

def lambda_handler(event, context):
    # Retrieve data from DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('CryptoPrices')
    response = table.scan()
    items = response['Items']
    
    # Group items by CryptoSymbol
    symbol_groups = defaultdict(list)
    for item in items:
        symbol = item.get('CryptoSymbol')
        if symbol:
            symbol_groups[symbol].append(item)
    
    # Perform data analysis for each CryptoSymbol
    for symbol, group in symbol_groups.items():
        # Extract prices from the group
        prices = [item.get('Price', 0) for item in group]
        
        # Remove items with missing prices
        prices = [price for price in prices if price != 0]
        
        if len(prices) < 2:
            print(f"Not enough price data available for analysis for {symbol}.")
            continue
        
        # Perform data analysis
        mean_price = statistics.mean(prices)
        median_price = statistics.median(prices)
        std_dev = statistics.stdev(prices)
        
        # Print or store the analyzed data for each CryptoSymbol
        print(f"Data analysis for {symbol}:")
        print("Mean Price:", mean_price)
        print("Median Price:", median_price)
        print("Standard Deviation:", std_dev)
        
        # Additional analysis or data processing steps can be performed here
        
    # Return a response if necessary
    return {
        'statusCode': 200,
        'body': 'Data analysis completed successfully.'
    }