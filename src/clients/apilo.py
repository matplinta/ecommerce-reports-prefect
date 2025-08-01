import base64
from datetime import datetime, timedelta

import pytz
import requests

from .abstract_client import AbstractClient
from src.domain.entities import Order, OrderItem, Product, Marketplace, Offer
from src.utils import code_to_country, convert_to_pln


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
            
    OFFER_STATUS_MAP = {
        2: "Active",              # Aktywna
        66: "Creating (errored)", # Tworzenie
        67: "Creating",           # Tworzenie
        80: "Ended",              # Zakończona
        81: "Ended (No status)",  # Zakończona (brak stanu)
        82: "Ended (manually)",   # Zakończona (ręcznie)
        83: "Ended (naturally)",  # Zakończona (naturalnie)
        89: "Archived",           # Archiwum
    }
    
    def is_active_offer(self, status: int):
        """Check if the offer is active based on its status."""
        return status in [2]

    @property
    def platform_origin(self) -> str:
        return "Apilo"
            
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
            
    def _fetch_paginated(self, path: str, limit: int = 512, response_key: str = None, **additional_params):
        """
        Fetch all pages of results from an API endpoint that supports offset/limit pagination.
        
        Args:
            path: API subpath (e.g., "sale/auction")
            limit: Number of items per page
            response_key: Key in the response that contains the items list
                        (if None, assumes the entire response is the list)
            **additional_params: Any additional query parameters to include
        
        Returns:
            List of all items across all pages
        """
        query_params = {"limit": limit, **additional_params}
        all_items = []
        offset = 0
        
        while True:
            query_params["offset"] = offset
            response = self._make_request(path=path, query_params=query_params)
            
            # Extract items based on whether response_key is provided
            if response_key:
                items = response.get(response_key, [])
            else:
                items = response if isinstance(response, list) else []
            
            if len(items) == 0:
                break
                
            all_items.extend(items)
            offset += len(items)
            
        return all_items
            
    def get_offers(self):
        """Returns:
        [
            {'id': 48,
            'idExternal': '13673',
            'name': 'SZKŁO DO SAMSUNG GALAXY TAB',
            'status': 89,
            'startedAt': '2023-02-01T13:36:26+0100',
            'endedAt': None,
            'preferences': {'invoiceType': 'vatInvoice',
            'dispatchTime': {'unit': 'day', 'period': 1}},
            'platformAccount': {'id': 7, 'login': 'kontakt@gmail.com'},
            'auctionProducts': [{'id': 48,
                'sku': '516',
                'ean': '59038',
                'quantitySelling': 97853,
                'handlingTime': None,
                'priceWithTax': '22.99',
                'isWarehouseCheck': 0,
                'product': {'id': 861, 'sku': '516'}}
                ]
            },
            ...
        ]
        """
        return self._fetch_paginated(path="sale/auction", limit=512, response_key="auctions")
    
    def get_products(self):
        """Returns:
        [
            {'id': 1234,
            'sku': '7240',
            'ean': '11111111262',
            'name': 'RADIO SAMSUNG',
            'unit': None,
            'weight': None,
            'quantity': 9999,
            'priceWithTax': '1590.00',
            'priceWithoutTax': '1292.68',
            'tax': '23.00',
            'originalCode': None,
            'status': 1},
            ...
        ]
        """
        return self._fetch_paginated(path="warehouse/product", limit=2000, response_key="products")
    
    def get_products_media(self, only_main: bool = True):
        """Returns:
        [
            {'id': 42,
            'isMain': 1,
            'productId': 1,
            'uuid': '4d8f05ee-1111-468a-ae90-581825e1c5aa',
            'extension': 'jpeg',
            'link': 'https://xxxxxxxxxx.googleapis.com/xxxx/4d8f05ee-1111-468a-ae90-581825e1c5aa.jpeg'},
            ...
        ]
        """
        only_main = 1 if only_main else 0
        return self._fetch_paginated(path="warehouse/product/media", limit=512, response_key="media", onlyMain=only_main)
    
    def get_products_with_media(self):
        """
        Returns a list of products with their main media information.
        
        Returns:
            List of dictionaries with the following keys:
            - sku: Product SKU
            - product_id: Internal product ID
            - name: Product name
            - uuid: Media UUID (if available)
            - link: Media link URL (if available)
            - priceWithTax: Product price including tax
            e.g.
            {'sku': '7517',
            'product_id': 123,
            'name': 'RADIO LTE',
            'uuid': 'c8ad2978-xxxx-48e5-ba9a-2ef250ee3869',
            'link': 'https://prod.storage.googleapis.com/xxxx/c8ad2978-xxxx-48e5-ba9a-2ef250ee3869.jpeg',
            'priceWithTax': '1590.00'},
        """
        # Get all products
        products = self.get_products()
        
        # Get all main product images
        media_items = self.get_products_media(only_main=True)
        
        # Create a dictionary of media indexed by product ID for quick lookup
        media_by_product_id = {}
        for media in media_items:
            if media["isMain"] == 1:  # Only use main images
                media_by_product_id[media["productId"]] = media
        
        # Combine product data with media data
        result = []
        for product in products:
            product_id = product["id"]
            media = media_by_product_id.get(product_id, {})
            
            result.append({
                "sku": product["sku"],
                "product_id": product_id,
                "name": product["name"],
                "uuid": media.get("uuid"),
                "link": media.get("link"),
                "priceWithTax": product["priceWithTax"]
            })
        
        return result
    
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
        
    def get_marketplaces(self):
        """Returns a dictionary of marketplaces (order sources) by ID.
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
            for market in sources.get("platforms", []) #if market["description"].lower() != "manualaccount"
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
        
        query_params = {"createdAfter": date_from}
        if date_to is not None:
            date_to = self.__class__.format_datetime_iso8601(date_to)
            query_params["createdBefore"] = date_to

        return self._fetch_paginated(
            path="orders",
            limit=limit,
            response_key="orders",
            **query_params
        )
        
    
    def get_offers_in_domain_format(self) -> list[Offer]:
        """Returns offers in a domain format."""
        offers = self.get_offers()
        return self._to_domain_offers(offers)

    
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


        sources = self.get_marketplaces()
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
        
        sources = self.get_marketplaces()
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
                    price_pln=convert_to_pln(float(item["originalPriceWithTax"]), currency, exchange_rates),
                    quantity=int(item["quantity"]),
                )
                for item in order["orderItems"] if item["type"] != 2 and item["sku"] is not None  # Exclude delivery items
            ]
            
            domain_orders.append(
                Order(
                    external_id=order_id,
                    total_gross_original=total_paid_gross,
                    total_gross_pln=convert_to_pln(total_paid_gross, currency, exchange_rates),
                    delivery_cost_original=delivery_cost,
                    delivery_cost_pln=convert_to_pln(delivery_cost, currency, exchange_rates),
                    delivery_method=delivery_item["originalName"] if delivery_item else None,
                    currency=currency,
                    status=order_status_name,
                    country=country,
                    city=city,
                    created_at=created_at,
                    marketplace_extid=str(source_id),
                    marketplace_name=source_custom_name,
                    platform_origin="Apilo",
                    marketplace_type=source_type,
                    items=order_items,
                )
            )
        return domain_orders

    def _to_domain_products(self, products) -> dict:
        """Converts products to a domain format for easier processing.
        Format is a list of Product objects.
        """
        # Get all main product images
        media_items = self.get_products_media(only_main=True)
        
        # Create a dictionary of media indexed by product ID for quick lookup
        media_by_product_id = {}
        for media in media_items:
            if media["isMain"] == 1:  # Only use main images
                media_by_product_id[media["productId"]] = media
        
        # Combine product data with media data
        domain_products = {}
        for product in products:
            if not product["sku"]:
                continue
            
            product_id = product["id"]
            image_url = media_by_product_id.get(product_id, {})

            domain_products[product["sku"]] = Product(
                sku=product["sku"],
                name=product["name"],
                image_url=image_url.get("link") if image_url else None,
            )
        return domain_products

    def _to_domain_offers(self, offers) -> list[Offer]:
        """Converts offers to a domain format for easier processing.
        Format is a list of Offer objects.
        """
        domain_offers = []
        marketplaces = self.get_marketplaces()
        for offer in offers:
            if not offer["idExternal"] or len(offer["auctionProducts"]) != 1:
                continue
            
            started_at = None
            if offer["startedAt"]:
                started_at = datetime.fromisoformat(offer["startedAt"])
                started_at = started_at.astimezone(tz=self.timezone)
                
            ended_at = None
            if offer["endedAt"]:
                ended_at = datetime.fromisoformat(offer["endedAt"])
                ended_at = ended_at.astimezone(tz=self.timezone)
                
            product = offer["auctionProducts"][0]
            sku = product["sku"]
            ean = product["ean"]
            quantity_selling = product["quantitySelling"]
            price_with_tax = float(product["priceWithTax"])
            marketplace_extid = offer["platformAccount"]["id"]
            marketplace_type = marketplaces.get(marketplace_extid).get("type")
            marketplace_name = marketplaces.get(marketplace_extid).get("name")
            marketplace_type, marketplace_name = marketplaces[marketplace_extid]["type"], marketplaces[marketplace_extid]["name"]
            marketplace_default_name = f"{marketplace_type} - {marketplace_name}"
            marketplace_custom_name = self.marketplace_rename_map.get(marketplace_default_name, marketplace_default_name)

            domain_offers.append(
                Offer(
                    external_id=str(offer["id"]),
                    origin_id=str(offer["idExternal"]),
                    name=offer["name"],
                    started_at=started_at,
                    ended_at=ended_at,
                    quantity_selling=quantity_selling,
                    sku=sku,
                    ean=ean,
                    marketplace_extid=str(marketplace_extid),
                    marketplace_type=marketplace_type,
                    marketplace_name=marketplace_custom_name,
                    platform_origin=self.platform_origin, 
                    price_with_tax=price_with_tax,
                    status_id=offer["status"],
                    status_name=self.OFFER_STATUS_MAP.get(offer["status"], "Unknown"),
                    is_active=self.is_active_offer(offer["status"]),
                )
            )
        return domain_offers