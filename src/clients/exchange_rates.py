import requests
from datetime import datetime, timedelta
from time import sleep


class ExchangeRateApiException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"ExchangeRateApiException: {self.message}"


class ExchangeRateRapidApi:
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
        raise ExchangeRateApiException(
            f"Error converting {amount} {from_currency} to {to_currency}: {response.json()}"
        )

    def get_exchange_rates(self, from_currency="PLN", to_currencies="CZK,EUR"):
        url = f"https://{self.host}/latest"
        querystring = {"from": from_currency, "to": to_currencies}
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.host}
        response = requests.get(url, headers=headers, params=querystring)
        if response.json()["success"]:
            return response.json()


class ExchangeRateNbpApi:
    """
    Fetches exchange rates from the NBP public API.
    """

    BASE_URL = "https://api.nbp.pl/api"

    def get_exchange_rates(self, from_currency="PLN", to_currencies="CZK,EUR,HUF,RON", table="A"):
        """
        Fetches latest exchange rates for given currencies relative to from_currency.
        Only PLN as base is supported by NBP API.
        Returns a dict: {currency_code: rate, ...}
        """
        if from_currency != "PLN":
            raise ValueError("NBP API only supports PLN as base currency.")
        url = f"{self.BASE_URL}/exchangerates/tables/{table}/?format=json"
        response = requests.get(url)
        if response.status_code != 200:
            raise ExchangeRateApiException(f"NBP API error: {response.text}")
        data = response.json()
        rates = data[0]["rates"]
        to_currencies_set = set([currency.strip().upper() for currency in to_currencies.split(",")])
        result = {}
        for rate in rates:
            code = rate["code"]
            if code in to_currencies_set:
                result[code] = rate["mid"]
        return result

    def get_latest_exchange_rate(self, currency, table="A"):
        """
        Fetches the latest exchange rate for a single currency (relative to PLN).
        """
        url = f"{self.BASE_URL}/exchangerates/rates/{table}/{currency}/?format=json"
        response = requests.get(url)
        if response.status_code != 200:
            raise ExchangeRateApiException(f"NBP API error: {response.text}")
        data = response.json()
        return data["rates"][0]["mid"]

    def get_exchange_rates_for_date(
        self, date: str, currencies: str = "CZK,EUR,RON,HUF", table: str = "A", max_retries: int = 7
    ) -> dict:
        """
        Fetch exchange rates for multiple currencies for a given date.
        If data for the date is unavailable, tries previous days (up to max_retries).
        Args:
            date: Date in "YYYY-MM-DD" format.
            currencies: Comma-separated string of currency codes, e.g. "CZK,EUR,RON,HUF".
            table: NBP table type (default "A").
            max_retries: How many days back to try (default 7).
        Returns:
            Dict of {currency_code: rate}.
        Raises:
            ExchangeRateApiException if data not found after retries or other errors.
        """
        url_template = f"{self.BASE_URL}/exchangerates/tables/{table}/{{}}/?format=json"
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        currencies_set = set(currency.strip().upper() for currency in currencies.split(","))
        for _ in range(max_retries):
            url = url_template.format(date_obj.strftime("%Y-%m-%d"))
            response = requests.get(url)
            if response.status_code == 200:
                rates = response.json()[0]["rates"]
                return {rate["code"]: rate["mid"] for rate in rates if rate["code"] in currencies_set}
            elif response.status_code == 404:
                print(f"404 for {date_obj.strftime('%Y-%m-%d')}, trying previous day...")
                date_obj -= timedelta(days=1)
                sleep(0.2)
            else:
                raise ExchangeRateApiException(f"NBP API error: {response.text}")
        raise ExchangeRateApiException(f"No NBP data found for {date} or previous {max_retries} days.")
