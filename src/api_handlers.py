import base64
import json
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests

TIMEZONE = pytz.timezone("Europe/Warsaw")


class ApiHandlerBase:
    @staticmethod
    def drop_empty_or_duplicates_sku(df_source):
        df = df_source[df_source["sku"] != ""]
        df = df.drop_duplicates(subset="sku", keep="first")
        return df

    @staticmethod
    def get_epoch_x_days_from_now(days=30):
        now = datetime.now(TIMEZONE)
        days_ago = now - timedelta(days=days)
        days_ago_start = days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Dane od {days_ago_start}")
        return int(days_ago_start.timestamp())

    @staticmethod
    def format_datetime_iso8601(dt):
        if dt.tzinfo is None:
            raise ValueError("Datetime object must be timezone-aware")
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


class ApiloApi(ApiHandlerBase):
    def __init__(
        self, client_id, client_secret, auth_code, url, token=None, refresh_token=None
    ) -> None:
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
            super().__init__(f"{message} - Status Code: {status_code}, Response Text: {response_text}")

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
            raise self.APIRequestError(response.status_code, response.text, "HTTP request failed")

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

        def group_data_by_description(data):
            """Make the output as baselinker format"""
            result = {}
            for item in data:
                description = item.get("description", "unknown").lower()
                if description not in result:
                    result[description] = {}
                result[description][str(item["id"])] = item["name"]
            return result

        response = self._make_request(path="sale")
        return group_data_by_description(response["platforms"])

    def get_orders(self, date_from: str, date_to: str = None, limit=512):
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

    def get_sell_statistics_dataframe(
        self,
        conversion_rates,
        previous_days: int = None,
        date_range: str = None,
        **kwargs,
    ) -> pd.DataFrame:
        def convert_date_range(date_range_str):
            start_date_str, end_date_str = date_range_str.split(" - ")
            date_format = "%d/%m/%Y"
            start_date = datetime.strptime(start_date_str, date_format)
            end_date = datetime.strptime(end_date_str, date_format)
            start_date = TIMEZONE.localize(start_date)
            end_date = TIMEZONE.localize(end_date)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
            return start_date, end_date

        def get_local_datetime_from_epoch(epoch_timestamp):
            utc_naive_datetime = datetime.utcfromtimestamp(epoch_timestamp)
            return pytz.utc.localize(utc_naive_datetime).astimezone(TIMEZONE)

        def convert_to_target_currency(row, exchange_rates, target_currencies):
            if row["currency"] == target_currencies[row["source"]]:
                return row["gross_order_price_wo_delivery"]
            elif row["currency"] in exchange_rates:
                return (
                    row["gross_order_price_wo_delivery"]
                    * exchange_rates[row["currency"]]
                )
            else:
                raise ValueError(f"Unsupported currency: {row['currency']}")

        def find_by_id(data_dict, search_id):
            search_id = str(search_id)  # make sure ID is string
            for outer_key, inner_dict in data_dict.items():
                if search_id in inner_dict:
                    return outer_key, inner_dict[search_id]
            return None  # or raise exception if preferred

        def get_delivery_item(order_items):
            for item in order_items:
                if item.get("type") == 2:
                    return item
            return None

        if date_range:
            date_from, date_to = convert_date_range(date_range)
        else:
            if previous_days is None:
                previous_days = 1
            now = datetime.now(TIMEZONE)
            days_ago = now - timedelta(days=previous_days)
            date_from = days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday = now - timedelta(days=1)
            date_to = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

        print(f"Date from {date_from} to {date_to}")
        date_from = self.__class__.format_datetime_iso8601(date_from)
        sources = self.get_order_sources()

        orders = self.get_orders(date_from=date_from, date_to=date_to)
        simplified_orders = []
        for order in orders:
            order_status = order["status"]
            if order_status == 21:  # Cancelled
                continue
            order_id = order["id"]
            source_id = str(order["platformAccountId"])
            source, source_name = find_by_id(sources, source_id)
            delivery_item = get_delivery_item(order["orderItems"])
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
                "source": f"{source} - {source_name}",
                "order_id": order_id,
                "total_paid": payment_done,
                "delivery_price": delivery_price,
                "currency": order["originalCurrency"],
            }
            simplified_orders.append(simplified_order)

        df = pd.DataFrame(simplified_orders)
        # df['gross_order_price_wo_delivery'] = df['total_paid']
        df["gross_order_price_wo_delivery"] = df["total_paid"] - df["delivery_price"]

        # target_currencies = df.groupby("source")["currency"].first().to_dict()
        target_currencies = {source: "PLN" for source in df['source'].unique()}

        df["gross_order_price_wo_delivery_pln"] = df.apply(
            convert_to_target_currency,
            axis=1,
            args=(conversion_rates, target_currencies),
        )
        df_grouped = df.groupby(["source"]).agg(
            order_count=("source", "size"),  # Count the number of orders
            total_net_payment_in_default_currency=(
                "gross_order_price_wo_delivery_pln",
                "sum",
            ),
            currency=("currency", "first"),
        )
        for source, target_currency in target_currencies.items():
            if source in df_grouped.index:
                df_grouped.at[source, 'currency'] = target_currency
        
        return df_grouped, df


class BaselinkerApi(ApiHandlerBase):
    URL = "https://api.baselinker.com/connector.php"

    def __init__(self, token) -> None:
        self.token = token

    def _make_request(self, method, parameters=None) -> requests.Response:
        if parameters is None:
            parameters = {}
        headers = {"X-BLToken": self.token}
        payload = {"method": method, "parameters": json.dumps(parameters)}
        return requests.post(self.URL, headers=headers, data=payload).json()

    def get_order_sources(self):
        """Returns: (example data anonymized)
        {
            "personal": {
                "0": "Osobi\u015bcie/tel."
            },
            "shop": {
                "123123": "plus.cz",
                "123124": "plus.sk",
                "123125": "acplus.cz",
                "123126": "Gear.cz"
            },
            "allegro": {
                "123127": "plus_cz"
            },
            "kauflandcz": {
                "123128": "Kaufland"
            },
            "heureka": {
                "123129": "plus.cz"
            }
        }
        """
        response = self._make_request(method="getOrderSources")
        return dict(response["sources"])

    def get_orders(self, date_from: int = None, **kwargs):
        """
        List of (example data anonymized)
        {
            'order_id': 2314124,
            'shop_order_id': 0,
            'external_order_id': '12313213-502e-11ef-9561-9feec8e479e9',
            'order_source': 'allegro',
            'order_source_id': 2344,
            'order_source_info': '-',
            'order_status_id': 4444,
            'confirmed': True,
            'date_confirmed': 17222226486,
            'date_add': 1722522263,
            'date_in_status': 1722222761,
            'user_login': 'Client:1244xxxx',
            'phone': '+4205xxxxxxx',
            'email': 'xxxx@mail.com',
            'user_comments': '',
            'admin_comments': '',
            'currency': 'CZK',
            'payment_method': 'PayU',
            'payment_method_cod': '0',
            'payment_done': 448,
            'delivery_method': 'Allegro Automaty Paczkowe WE|DO',
            'delivery_price': 49,
            'delivery_package_module': 'allegrokurier',
            'delivery_package_nr': 'XXXXXXXXXXX1',
            'delivery_fullname': 'XXXX XXXXX',
            'delivery_company': '',
            'delivery_address': 'awdawd 30',
            'delivery_city': 'Mesto Mesto',
            'delivery_state': '',
            'delivery_postcode': '330 33',
            'delivery_country_code': 'CZ',
            'delivery_point_id': 'ALZAxxxx',
            'delivery_point_name': 'Alzaxxxxxx',
            'delivery_point_address': 'awdawd 21',
            'delivery_point_postcode': 'awdawd',
            'delivery_point_city': 'awdawd',
            'invoice_fullname': 'awd awd',
            'invoice_company': '',
            'invoice_nip': '',
            'invoice_address': 'awd awd',
            'invoice_city': 'awd awd',
            'invoice_state': '',
            'invoice_postcode': '330 11',
            'invoice_country_code': 'CZ',
            'want_invoice': '0',
            'extra_field_1': '',
            'extra_field_2': '',
            'order_page': 'https://orders-d.baselinker.com/xxxxx/xxxxx/',
            'pick_state': 0,
            'pack_state': 1,
            'delivery_country': 'Czechy',
            'invoice_country': 'Czechy',
            'products': [
                {
                    'storage': 'db',
                    'storage_id': 123213,
                    'order_product_id': 1234131,
                    'product_id': '123123213',
                    'variant_id': '0',
                    'name': 'Obal nepromokavý kryt na zahradní gril + pouzdro 140x65x115CM',
                    'attributes': '',
                    'sku': '1234',
                    'ean': '1232154125215',
                    'location': '1',
                    'warehouse_id': 12313,
                    'auction_id': '123123',
                    'price_brutto': 399,
                    'tax_rate': 21,
                    'quantity': 1,
                    'weight': 0,
                    'bundle_id': 0
                }
            ]
        }
        """
        if date_from is None:
            date_from = self.__class__.get_epoch_x_days_from_now()

        parameters = {"date_from": date_from, "get_unconfirmed_orders": True, **kwargs}
        orders = []
        while True:
            response = self._make_request(method="getOrders", parameters=parameters)
            dict_resp = dict(response)

            if (len_orders := len(dict_resp["orders"])) == 0:
                break

            orders.extend(dict_resp["orders"])

            if len_orders == 100:
                last_order_date = orders[-1]["date_add"]
                parameters["date_from"] = last_order_date + 1
            else:
                break

        return orders

    def get_inventory_products_list(self, inventory_id, page=None):
        parameters = {
            "inventory_id": inventory_id,
        }
        if page:
            parameters["page"] = page

        products = {}
        page = 1

        while True:
            response = self._make_request(
                method="getInventoryProductsList", parameters=parameters
            )
            if (len_products := len(response["products"])) == 0:
                break

            products = dict(products, **response["products"])

            if len_products == 1000:
                page += 1
                parameters["page"] = page
            else:
                break
        return products

    def get_inventory_products_data(self, inventory_id: int, products: list):
        def get_slices(lst, slice_size=1000):
            while lst:
                yield lst[:slice_size]
                lst = lst[slice_size:]

        parameters = {"inventory_id": inventory_id}
        prods_detailed = {}
        for slice in get_slices(products):
            parameters["products"] = slice
            response = self._make_request(
                method="getInventoryProductsData", parameters=parameters
            )
            prods_detailed = dict(prods_detailed, **response["products"])

        return prods_detailed

    def get_inventories(self):
        return self._make_request(method="getInventories")["inventories"]

    def get_inventory_warehouses(self):
        return self._make_request(method="getInventoryWarehouses")["warehouses"]

    @staticmethod
    def get_epoch_x_days_from_now(days=30):
        now = datetime.now(TIMEZONE)
        days_ago = now - timedelta(days=days)
        days_ago_start = days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Dane od {days_ago_start}")
        return int(days_ago_start.timestamp())

    @staticmethod
    def parse_products_data_to_dataframe(products: dict):
        flattened_data = []
        for product_id, data in products.items():
            if "images" in data and isinstance(data["images"], dict):
                image = None
                for i in range(1, len(data["images"]) + 1):
                    key = str(i)
                    if key in data["images"]:
                        image = data["images"][key]
                        break
            else:
                image = None
            selected_data = {
                "product_id": product_id,
                "sku": data["sku"],
                "ean": data["ean"],
                "name": data["text_fields"]["name"]
                if data["text_fields"] and "name" in data["text_fields"]
                else None,
                "quantity": sum(data["stock"].values()),
                "image": image,
            }
            flattened_data.append(selected_data)

        # Step 2: Create a DataFrame
        df = pd.DataFrame(flattened_data)
        df["sku"] = df["sku"].astype(str)
        df = __class__.drop_empty_or_duplicates_sku(df)

        # Step 3: Set the SKU as the index
        df.set_index("sku", inplace=True)
        return df

    def get_all_products_dataframe(self) -> pd.DataFrame:
        inventory_id = self.get_inventories()[0]["inventory_id"]
        products = self.get_inventory_products_list(inventory_id=inventory_id)
        products = self.get_inventory_products_data(
            inventory_id=inventory_id, products=list(products.keys())
        )
        products_df = self.__class__.parse_products_data_to_dataframe(products)
        return products_df

    def get_sold_quantity_by_source_dataframe(
        self, date_from: int = None, **kwargs
    ) -> pd.DataFrame:
        orders = self.get_orders(date_from=date_from, **kwargs)
        sources = self.get_order_sources()

        data = []
        for order in orders:
            source = order["order_source"]
            source_id = str(order["order_source_id"])
            source_name = sources[source].get(source_id, f"{source} - {source_id}")

            for product in order["products"]:
                data.append(
                    {
                        "sku": product["sku"],
                        "source": f"{source} - {source_name}",
                        "sold_quantity": product["quantity"],
                    }
                )

        df = pd.DataFrame(data)

        # Step 3: Group by SKU and Order Source, and aggregate by summing quantities
        df_grouped = df.groupby(["sku", "source"]).sum().reset_index()

        # Step 4: Pivot the DataFrame to get SKUs as index and Order Sources as columns
        df_pivot = df_grouped.pivot(
            index="sku", columns="source", values="sold_quantity"
        ).fillna(0)
        df_pivot = df_pivot.astype(int)
        return df_pivot

    def get_complete_products_info_with_sell_info_dataframe(
        self, date_from: int = None, **kwargs
    ) -> pd.DataFrame:
        products_df = self.get_all_products_dataframe()
        sell_df = self.get_sold_quantity_by_source_dataframe(
            date_from=date_from, **kwargs
        )
        # df_combined = products_df.join(sell_df, how='left')
        df_combined = pd.merge(
            products_df, sell_df, on="sku", how="outer", suffixes=("_sub", "_base")
        )

        df_combined[sell_df.columns] = df_combined[sell_df.columns].fillna(0)
        df_combined[sell_df.columns] = df_combined[sell_df.columns].astype(int)
        return df_combined

    def get_sell_statistics_dataframe(
        self,
        conversion_rates,
        previous_days: int = None,
        date_range: str = None,
        **kwargs,
    ) -> pd.DataFrame:
        def convert_date_range(date_range_str):
            start_date_str, end_date_str = date_range_str.split(" - ")
            date_format = "%d/%m/%Y"
            start_date = datetime.strptime(start_date_str, date_format)
            end_date = datetime.strptime(end_date_str, date_format)
            start_date = TIMEZONE.localize(start_date)
            end_date = TIMEZONE.localize(end_date)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
            return start_date, end_date

        def get_local_datetime_from_epoch(epoch_timestamp):
            utc_naive_datetime = datetime.utcfromtimestamp(epoch_timestamp)
            return pytz.utc.localize(utc_naive_datetime).astimezone(TIMEZONE)

        def convert_to_target_currency(row, exchange_rates, target_currencies):
            if row["currency"] == target_currencies[row["source"]]:
                return row["gross_order_price_wo_delivery"]
            elif row["currency"] in exchange_rates:
                return (
                    row["gross_order_price_wo_delivery"]
                    * exchange_rates[row["currency"]]
                )
            else:
                raise ValueError(f"Unsupported currency: {row['currency']}")

        if date_range:
            date_from, date_to = convert_date_range(date_range)
        else:
            if previous_days is None:
                previous_days = 1
            now = datetime.now(TIMEZONE)
            days_ago = now - timedelta(days=previous_days)
            date_from = days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday = now - timedelta(days=1)
            date_to = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

        print(f"Date from {date_from} to {date_to}")
        date_from = int(date_from.timestamp())
        sources = self.get_order_sources()
        orders = self.get_orders(date_from=date_from, **kwargs)
        simplified_orders = []
        for order in orders:
            order_date = get_local_datetime_from_epoch(order["date_add"])
            if order_date >= date_to:
                continue

            order_status = order["order_status_id"]
            if order_status == 196511:  # Anulowane
                continue
            
            if "products" not in order.keys():
                continue

            order_id = order["order_id"]
            source = order["order_source"]
            source_id = str(order["order_source_id"])
            source_name = sources[source].get(source_id, f"{source} - {source_id}")
            payment_done = float(order["payment_done"])
            delivery_price = float(order["delivery_price"])
            if payment_done == 0:
                payment_done = delivery_price + sum(
                    [
                        product["price_brutto"] * product["quantity"]
                        for product in order["products"]
                    ]
                )
            simplified_order = {
                "source": f"{source} - {source_name}",
                "order_id": order_id,
                "total_paid": payment_done,
                "delivery_price": delivery_price,
                "currency": order["currency"],
            }
            simplified_orders.append(simplified_order)

        df = pd.DataFrame(simplified_orders)
        df["gross_order_price_wo_delivery"] = df["total_paid"] - df["delivery_price"]
        # df['gross_order_price_wo_delivery'] = df['total_paid']

        # target_currencies = df.groupby("source")["currency"].first().to_dict()
        target_currencies = {source: "PLN" for source in df['source'].unique()}

        df["gross_order_price_wo_delivery_pln"] = df.apply(
            convert_to_target_currency,
            axis=1,
            args=(conversion_rates, target_currencies),
        )
        df_grouped = df.groupby(["source"]).agg(
            order_count=("source", "size"),  # Count the number of orders
            total_net_payment_in_default_currency=(
                "gross_order_price_wo_delivery_pln",
                "sum",
            ),
            currency=("currency", "first"),
        )
        for source, target_currency in target_currencies.items():
            if source in df_grouped.index:
                df_grouped.at[source, 'currency'] = target_currency
                
        return df_grouped, df
