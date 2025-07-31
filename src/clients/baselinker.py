import json
from datetime import datetime, timedelta

import pandas as pd
import requests
import pytz


from .abstract_client import AbstractClient
from src.domain.entities import Order, OrderItem, Product, Marketplace
from src.utils import code_to_country, convert_to_pln


class BaselinkerClient(AbstractClient):
    URL = "https://api.baselinker.com/connector.php"

    def __init__(self, token, timezone=pytz.timezone("Europe/Warsaw"), order_status_ids_to_ignore=None, marketplace_rename_map=None) -> None:
        super().__init__(timezone, order_status_ids_to_ignore, marketplace_rename_map)
        self.token = token
        
    @property
    def platform_origin(self) -> str:
        return "Baselinker"

    def _make_request(self, method, parameters=None) -> requests.Response:
        if parameters is None:
            parameters = {}
        headers = {"X-BLToken": self.token}
        payload = {"method": method, "parameters": json.dumps(parameters)}
        return requests.post(self.URL, headers=headers, data=payload).json()

    def get_order_status_types(self):
        """Returns a dictionary of order status types. Consists of status ID as key and status name as value."""
        response = self._make_request(method="getOrderStatusList")
        return {elem["id"]: elem["name"] for elem in response["statuses"]}

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
            source_id: {"name": name, "type": market_type}
            for market_type, id_name_map in sources.items()
            for source_id, name in id_name_map.items()
        }

    def get_orders(self, date_from: datetime, date_to: datetime = None, **kwargs):
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
        date_from_epoch = int(date_from.timestamp())

        parameters = {"date_from": date_from_epoch, "get_unconfirmed_orders": True, **kwargs}
        orders = []
        while True:
            response = self._make_request(method="getOrders", parameters=parameters)
            dict_resp = dict(response)

            if (len_orders := len(dict_resp["orders"])) == 0:
                break

            # orders.extend(dict_resp["orders"])
            # Filter orders by date range
            for order in dict_resp["orders"]:
                order_date = datetime.fromtimestamp(order["date_add"], tz=self.timezone)
                if date_from < order_date < date_to:
                    orders.append(order)

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
        """Returns: 
        [
            {'inventory_id': 26286,
            'name': 'Domyślny',
            'description': '',
            'languages': ['cs'],
            'default_language': 'cs',
            'price_groups': [23841],
            'default_price_group': 23841,
            'warehouses': ['bl_33833', 'bl_60498', 'bl_70162'],
            'default_warehouse': 'bl_33833',
            'reservations': False,
            'is_default': True}
        ]
        """
        return self._make_request(method="getInventories")["inventories"]

    def get_inventory_warehouses(self):
        return self._make_request(method="getInventoryWarehouses")["warehouses"]
    
    def get_products(self, inventory: int=None) -> list[dict]:
        """If no inventory is provided, it will use the default inventory.
        """
        def get_default_inventory_id(inventories):
            for inv in inventories:
                if inv.get("is_default"):
                    return inv.get("inventory_id")
            return None
        
        inventories = self.get_inventories()
        if inventory is None:
            inventory_id = get_default_inventory_id(inventories)
        products = self.get_inventory_products_list(inventory_id=inventory_id)
        products = self.get_inventory_products_data(inventory_id=inventory_id, products=list(products.keys()))
        return products


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
    
    def _to_simplified_orders(self, orders):
        """Converts orders to a simplified format for easier processing.
        Format is a list of dictionaries with keys:
            {"source", "order_id", "total_paid", "delivery_price", "currency"}
        """
        sources = self.get_marketplaces()
        simplified_orders = []
        for order in orders:
            order_date = datetime.fromtimestamp(order["date_add"], tz=self.timezone)
            order_status = order["order_status_id"]
            if self._should_ignore_order(order_status):
                continue

            if "products" not in order.keys():
                continue

            order_id = order["order_id"]
            source_type = order["order_source"]
            source_id = str(order["order_source_id"])
            source_type, source_name = sources[source_id]["type"], sources[source_id]["name"]
            source_default_name = f"{source_type} - {source_name}"
            source_custom_name = self.marketplace_rename_map.get(source_default_name, source_default_name)
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
                "source": source_custom_name,
                "order_id": order_id,
                "total_paid": payment_done,
                "delivery_price": delivery_price,
                "currency": order["currency"],
            }
            simplified_orders.append(simplified_order)
        return simplified_orders
    
    
    def _to_domain_orders(self, orders, exchange_rates):
        """Converts orders to a canonical format for easier processing.
        Format is a list of OrderCanonical objects.
        """
        
        sources = self.get_marketplaces()
        status_types = self.get_order_status_types()
        domain_orders = []
        for order in orders:
            order_status_id = order["order_status_id"]
            order_status_name = status_types.get(order_status_id, None).capitalize()
            if self._should_ignore_order(order_status_id):
                continue

            if "products" not in order.keys():
                continue
            
            country = order.get("delivery_country_code", None)
            if country:
                country = code_to_country(country)
            city = order.get("delivery_city", None)

            order_id = order["order_id"]
            source_type = order["order_source"]
            source_id = str(order["order_source_id"])
            source_type, source_name = sources[source_id]["type"], sources[source_id]["name"]
            source_default_name = f"{source_type} - {source_name}"
            source_custom_name = self.marketplace_rename_map.get(source_default_name, source_default_name)
            
            created_at = datetime.fromtimestamp(order["date_add"], tz=self.timezone)
            currency = order["currency"].upper()
            total_paid_gross = float(order["payment_done"])
            delivery_cost = float(order["delivery_price"])
            if total_paid_gross == 0:
                total_paid_gross = delivery_cost + sum(
                    [
                        product["price_brutto"] * product["quantity"]
                        for product in order["products"]
                    ]
                )
            
            order_items = [
                OrderItem(
                    sku=item["sku"],
                    name=item["name"],
                    price=float(item["price_brutto"]),
                    price_pln=convert_to_pln(float(item["price_brutto"]), currency, exchange_rates),
                    quantity=int(item["quantity"]),
                )
                for item in order["products"]
            ]
            
            domain_orders.append(
                Order(
                    external_id=str(order_id),
                    total_gross_original=total_paid_gross,
                    total_gross_pln=convert_to_pln(total_paid_gross, currency, exchange_rates),
                    delivery_cost_original=delivery_cost,
                    delivery_cost_pln=convert_to_pln(delivery_cost, currency, exchange_rates),
                    delivery_method=order.get("delivery_method", None),
                    currency=currency,
                    status=order_status_name,
                    country=country,
                    city=city,
                    created_at=created_at,
                    marketplace_extid=str(source_id),
                    marketplace_name=source_custom_name,
                    platform_origin="Baselinker",
                    marketplace_type=source_type,
                    items=order_items,
                )
            )
        return domain_orders

    def _to_domain_products(self, products) -> dict:
        """Converts products to a domain format for easier processing.
        Format is a list of Product objects.
        """
        domain_products = {}
        for product_id, data in products.items():
            if not data["sku"]:
                continue
            if "images" in data and isinstance(data["images"], dict):
                image = None
                for i in range(1, len(data["images"]) + 1):
                    key = str(i)
                    if key in data["images"]:
                        image = data["images"][key]
                        break
            else:
                image = None

            domain_products[data["sku"]] = Product(
                sku=data["sku"],
                name=data["text_fields"].get("name", ""),
                image_url=image
            )
        return domain_products