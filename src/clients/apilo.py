import base64
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests

from .abstract_client import AbstractClient
from src.domain.entities import Order, OrderItem
from src.utils import code_to_country


class ApiloClient(AbstractClient):
    def __init__(
        self,
        client_id,
        client_secret,
        auth_code,
        url,
        token=None,
        refresh_token=None,
        timezone=pytz.timezone("Europe/Warsaw"),
        order_status_ids_to_ignore=None,
        marketplace_rename_map=None,
    ) -> None:
        super().__init__(timezone, order_status_ids_to_ignore, marketplace_rename_map)
        credentials = f"{client_id}:{client_secret}"
        self.url = url
        self.auth_code = auth_code
        self.token = token
        self.refresh_token = refresh_token
        self.encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        if token is None or str(token) == "-1":
            self.obtain_access_token()
            
    @staticmethod
    def format_datetime_iso8601(dt: datetime):
        if dt.tzinfo is None:
            raise ValueError("Datetime object must be timezone-aware")
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    def _send_token_request(self, type: str):
        if type == "refresh":
            payload = {"grantType": "refresh_token", "token": self.refresh_token}
        else:
            payload = {"grantType": "authorization_code", "token": self.auth_code}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.encoded_credentials}",
        }
        request_url = f"{self.url}/rest/auth/token/"
        return requests.post(request_url, json=payload, headers=headers)

    def obtain_access_token(self):
        response = self._send_token_request(type="authorization_code")
        if response.status_code == 201:
            json_response = response.json()
            self.token = json_response.get("accessToken")
            self.refresh_token = json_response.get("refreshToken")
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def refresh_access_token(self):
        assert str(self.refresh_token) != "-1", "Refresh token is not set"
        response = self._send_token_request(type="refresh")
        if response.status_code == 201:
            json_response = response.json()
            self.token = json_response.get("accessToken")
            self.refresh_token = json_response.get("refreshToken")
            refresh_expiry = json_response.get("refreshTokenExpireAt")
            print(f"Refreshed token expire at: {refresh_expiry}")
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    class APIRequestError(Exception):
        """Custom exception for API request failures."""

        def __init__(self, status_code, response_text, message="API request failed"):
            self.status_code = status_code
            self.response_text = response_text
            self.message = message
            super().__init__(
                f"{message} - Status Code: {status_code}, Response Text: {response_text}"
            )

    def _make_request(self, query_params=None, path="") -> requests.Response:
        if query_params is None:
            query_params = {}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        request_url = f"{self.url}/rest/api/{path}"
        response = requests.get(request_url, headers=headers, params=query_params)
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise self.APIRequestError(
                response.status_code, response.text, "HTTP request failed"
            )

    def get_order_status_types(self):
        """Returns a dictionary of order status types. Consists of status ID as key and status name as value."""
        response = self._make_request(path="orders/status/map")
        return {elem["id"]: elem["name"] for elem in response}

    def get_order_sources(self):
        """Returns: (example data anonymized)
        {
            'platforms': [
                {'id': 1,
                'name': 'Allegro Vsklep',
                'alias': 'AL',
                'type': 11,
                'description': 'Allegro'},
                {'id': 2,
                'name': 'Empik kontakt@gmail.com',
                'alias': 'EM',
                'type': 32,
                'description': 'Empik'},
                {'id': 3,
                'name': 'Erli kontakt@gmail.com',
                'alias': 'ER',
                'type': 41,
                'description': 'Erli'},
                {'id': 4,
                'name': 'Gear',
                'alias': 'GG',
                'type': 35,
                'description': 'Woocommerce'},
                {'id': 5,
                'name': 'NewSolutions',
                'alias': 'NC',
                'type': 35,
                'description': 'Woocommerce'}
            ],
            'totalCount': 15
        }
        """

        response = self._make_request(path="sale")
        return response
        
    def get_order_sources_by_id(self):
        """Returns a dictionary of order sources by ID.
        Example data:
        {
            "123123": {"name": "plus.cz", "type": "Allegro"},
            "123124": {"name": "plus.sk", "type": "Erli"},
            "123125": {"name": "acplus.cz", "type": "shop"},
            "123126": {"name": "Gear.cz", "type": "Allegro"}
        }
        """
        sources = self.get_order_sources()
        return {
            market["id"]: {"name": market["name"], "type": market["description"].lower()}
            for market in sources.get("platforms", [])
        }

    def get_orders(self, date_from: datetime, date_to: datetime = None, limit=512):
        """Returns: (example data anonymized)
        {'orders': [{'platformAccountId': 27,
        'idExternal': '2414',
        'isInvoice': False,
        'paymentStatus': 0,
        'paymentType': 2,
        'originalCurrency': 'PLN',
        'isEncrypted': False,
        'createdAt': '2024-12-02T13:01:51+0100',
        'updatedAt': '2024-12-02T13:03:35+0100',
        'orderItems': [{'id': 255030,
            'idExternal': None,
            'ean': None,
            'sku': None,
            'originalName': 'Etui pasujące do Apple iPad 10.9 GEN 10 2022 A2757 A2696<span> - </span>Żółty',
            'originalCode': '358|369',
            'originalPriceWithTax': '49.00',
            'originalPriceWithoutTax': '39.84',
            'media': None,
            'quantity': 1,
            'tax': '23.00',
            'status': 1,
            'unit': None,
            'type': 1},
            {'id': 122213,
            'idExternal': None,
            'ean': None,
            'sku': None,
            'originalName': 'Przesyłka Kurierska InPost - płatność przy odbiorze',
            'originalCode': 'flat_rate|4',
            'originalPriceWithTax': '23.98',
            'originalPriceWithoutTax': '19.50',
            'media': None,
            'quantity': 1,
            'tax': '23.00',
            'status': 1,
            'unit': None,
            'type': 2}],
        'addressCustomer': {'name': 'test test',
            'phone': None,
            'email': 'jakub@gmail.com',
            'id': 12321,
            'streetName': 'stawowa',
            'streetNumber': '20',
            'city': 'aubiszczyn',
            'zipCode': '43-800',
            'country': 'PL',
            'department': '43-800',
            'class': 'house'},
        'platformId': 37,
        'isCanceledByBuyer': False,
        'id': 'ACawdawda0603',
        'status': 21},
        """
        date_from = self.__class__.format_datetime_iso8601(date_from)
        date_to = self.__class__.format_datetime_iso8601(date_to)
        
        query_params = {"createdAfter": date_from, "limit": limit}
        if date_to is not None:
            query_params["createdBefore"] = date_to

        all_orders = []
        offset = 0

        while True:
            query_params["offset"] = offset
            response = self._make_request(path="orders", query_params=query_params)

            if (len_orders := len(response.get("orders", []))) == 0:
                break

            all_orders.extend(response["orders"])
            offset += len_orders

        return all_orders
    
    @staticmethod
    def _get_delivery_item(order_items):
        for item in order_items:
            if item.get("type") == 2:
                return item
        return None

    def _to_simplified_orders(self, orders):
        """Converts orders to a simplified format for easier processing.
        Format is a list of dictionaries with keys:
            {"source", "order_id", "total_paid", "delivery_price", "currency"}
        """


        sources = self.get_order_sources_by_id()
        simplified_orders = []
        for order in orders:
            order_status = order["status"]
            if self._should_ignore_order(order_status):
                continue
            
            order_id = order["id"]
            source_id = order["platformAccountId"]
            source_type, source_name = sources[source_id]["type"], sources[source_id]["name"]
            source_default_name = f"{source_type} - {source_name}"
            source_custom_name = self.marketplace_rename_map.get(source_default_name, source_default_name)
            delivery_item = self.__class__._get_delivery_item(order["orderItems"])
            delivery_price = (
                float(delivery_item["originalPriceWithTax"]) if delivery_item else 0
            )
            payment_done = sum(
                [
                    float(product["originalPriceWithTax"]) * product["quantity"]
                    for product in order["orderItems"]
                ]
            )
            simplified_order = {
                "source": source_custom_name,
                "order_id": order_id,
                "total_paid": payment_done,
                "delivery_price": delivery_price,
                "currency": order["originalCurrency"],
            }
            simplified_orders.append(simplified_order)
        return simplified_orders

    def _to_domain_orders(self, orders, exchange_rates):
        """Converts orders to a domain format for easier processing.
        Format is a list of Order objects.
        """
        def convert_to_pln(price, currency):
            if currency == "PLN":
                return price
            if exchange_rates and currency in exchange_rates:
                return price * exchange_rates[currency]
            raise ValueError(f"Unsupported currency: {currency}")
        
        sources = self.get_order_sources_by_id()
        status_types = self.get_order_status_types()
        domain_orders = []
        for order in orders:
            order_status_id = order["status"]
            order_status_name = status_types.get(order_status_id, None).capitalize()
            if self._should_ignore_order(order_status_id):
                continue

            if "addressCustomer" in order.keys():
                country = order["addressCustomer"].get("country", None)
                city = order["addressCustomer"].get("city", None)
            else:
                country = None
                city = None
                
            if country:
                country = code_to_country(country)

            order_id = str(order["id"])
            source_id = order["platformAccountId"]
            source_type, source_name = sources[source_id]["type"], sources[source_id]["name"]
            source_default_name = f"{source_type} - {source_name}"
            source_custom_name = self.marketplace_rename_map.get(source_default_name, source_default_name)
            
            created_at = datetime.fromisoformat(order["createdAt"])
            created_at = created_at.astimezone(tz=self.timezone)
            currency = order["originalCurrency"].upper()
            delivery_item = self.__class__._get_delivery_item(order["orderItems"])
            delivery_cost = (
                float(delivery_item["originalPriceWithTax"]) if delivery_item else 0
            )
            total_paid_gross = sum(
                [
                    float(product["originalPriceWithTax"]) * product["quantity"]
                    for product in order["orderItems"]
                ]
            )
            
            order_items = [
                OrderItem(
                    sku=item["sku"],
                    name=item["originalName"],
                    price=float(item["originalPriceWithTax"]),
                    price_pln=convert_to_pln(float(item["originalPriceWithTax"]), currency),
                    quantity=int(item["quantity"]),
                )
                for item in order["orderItems"] if item["type"] != 2  # Exclude delivery items
            ]
            
            domain_orders.append(
                Order(
                    external_id=order_id,
                    total_gross_original=total_paid_gross,
                    total_gross_pln=convert_to_pln(total_paid_gross, currency),
                    delivery_cost_original=delivery_cost,
                    delivery_cost_pln=convert_to_pln(delivery_cost, currency),
                    delivery_method=delivery_item["originalName"] if delivery_item else None,
                    currency=currency,
                    status=order_status_name,
                    country=country,
                    city=city,
                    created_at=created_at,
                    marketplace_extid=str(source_id),
                    marketplace_name=source_custom_name,
                    items=order_items,
                )
            )
        return domain_orders


