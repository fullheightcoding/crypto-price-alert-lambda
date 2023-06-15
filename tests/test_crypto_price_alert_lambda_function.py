import unittest
# from crypto_price_alert_lambda import lambda_handler

class TestLambdaFunction(unittest.TestCase):

    def test_sum(self):
        self.assertEqual(sum([1, 2, 3]), 6, "Should be 6")

if __name__ == '__main__':
    unittest.main()