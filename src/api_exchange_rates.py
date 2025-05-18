import requests


class RapidApiException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"RapidApiException: {self.message}"


class ExchangeRateApi:
    def __init__(self, api_key, host):
        self.api_key = api_key
        self.host = host

    def convert_currency(self, amount=1, from_currency="CZK", to_currency="PLN"):
        url = f"https://{self.host}/convert"
        querystring = {"from": from_currency, "to": to_currency, "amount": str(amount)}
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.host}
        response = requests.get(url, headers=headers, params=querystring)
        if response.json()["success"]:
            return response.json()["result"]
        raise RapidApiException(
            f"Error converting {amount} {from_currency} to {to_currency}: {response.json()}"
        )

    def get_exchange_rates(self, from_currency="PLN", to_currencies="CZK,EUR"):
        url = f"https://{self.host}/latest"
        querystring = {"from": from_currency, "to": to_currencies}
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.host}
        response = requests.get(url, headers=headers, params=querystring)
        if response.json()["success"]:
            return response.json()
