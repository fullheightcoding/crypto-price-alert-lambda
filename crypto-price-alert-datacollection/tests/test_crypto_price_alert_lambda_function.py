import unittest
from unittest.mock import MagicMock, patch

import sys
import os
import asyncio

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

import CryptoPriceAlert

class TestCryptoPriceAlert(unittest.TestCase):
    # Rest of the test code...
    def test_main(self):
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(CryptoPriceAlert.main("bitcoin", 10000, "less_than"))
        self.assertEqual(result, "Price check complete.")

    def test_lambda_handler(self):
        event = {
            "threshold_coin": "bitcoin",
            "threshold_price": 10000,
            "threshold_direction": "less_than"
        }
        context = MagicMock()
        sns_client_mock = MagicMock()
        sns_client_mock.publish.return_value = {}
        with patch('crypto_price_alert_lambda.boto3.client') as boto3_client_mock:
            boto3_client_mock.return_value = sns_client_mock
            response = CryptoPriceAlert.lambda_handler(event, context)
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"], '"Price check complete."')

if __name__ == '__main__':
    unittest.main()
