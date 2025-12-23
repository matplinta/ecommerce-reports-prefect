from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import pandas as pd

from src.domain.entities import Order, OrderItem, Product, Marketplace

class AbstractClient(ABC):
    def __init__(self, timezone, order_status_ids_to_ignore=None, marketplace_rename_map=None):
        self.timezone = timezone
        self.order_status_ids_to_ignore = order_status_ids_to_ignore or []
        self.marketplace_rename_map = marketplace_rename_map or {}
        
    @property
    @abstractmethod
    def platform_origin(self) -> str:
        pass

    @staticmethod
    def drop_empty_or_duplicates_sku(df_source):
        df = df_source[df_source["sku"] != ""]
        df = df.drop_duplicates(subset="sku", keep="first")
        return df
    
    @staticmethod
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

    def resolve_date_range(
        self, previous_days: int=1, date_range: str=None
    ) -> tuple[datetime, datetime]:
        if date_range:
            fmt = "%d/%m/%Y"
            start_str, end_str = map(str.strip, date_range.split(" - "))
            date_from = self.timezone.localize(
                datetime.strptime(start_str, fmt).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            )
            date_to = self.timezone.localize(
                datetime.strptime(end_str, fmt).replace(
                    hour=23, minute=59, second=59, microsecond=0
                )
            )
            if date_from > date_to:
                raise ValueError("Start date cannot be after end date.")
        else:
            if previous_days is None:
                previous_days = 1
            now = datetime.now(tz=self.timezone)
            days_ago = now - timedelta(days=previous_days)
            date_from = days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday = now - timedelta(days=1)
            date_to = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
        return date_from, date_to
    
    def _should_ignore_order(self, order_status_id):
        """
        Determines if an order should be ignored based on its status ID.
        """
        if order_status_id in self.order_status_ids_to_ignore:
            return True
        return False
    
    def _summarize_orders(self, simplified_orders, conversion_rates):
        df = pd.DataFrame(simplified_orders)
        df["gross_order_price_wo_delivery"] = df["total_paid"] - df["delivery_price"]

        # target_currencies = df.groupby("source")["currency"].first().to_dict()
        target_currencies = {source: "PLN" for source in df["source"].unique()}

        df["gross_order_price_wo_delivery_pln"] = df.apply(
            self.__class__.convert_to_target_currency,
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
                df_grouped.at[source, "currency"] = target_currency

        return df_grouped, df
    
    
    def get_sell_statistics_dataframe(
        self,
        conversion_rates,
        previous_days: int = None,
        date_range: str = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Returns a DataFrame with sell statistics grouped by source.
        Args:
            conversion_rates (dict): A dictionary with currency conversion rates.
            previous_days (int, optional): Number of days to look back for orders. Defaults to None.
            date_range (str, optional): A date range in the format "dd/mm/yyyy - dd/mm/yyyy". Defaults to None.
            **kwargs: Additional parameters for the API request.
        """
        date_from, date_to = self.resolve_date_range(
            previous_days=previous_days, date_range=date_range
        )
        print(f"Date from {date_from} to {date_to}")
        orders = self.get_orders(date_from=date_from, date_to=date_to, **kwargs)
        simplified_orders = self._to_simplified_orders(orders)
        return self._summarize_orders(simplified_orders, conversion_rates)

    def get_marketplaces_in_domain_format(self) -> list[Marketplace]:
        """Returns marketplaces in a domain format."""
        marketplaces = self.get_marketplaces()
        return self._to_domain_marketplaces(marketplaces)

    def get_products_in_domain_format(self) -> list[dict]:
        """Returns products in a domain format."""
        products = self.get_products()
        return self._to_domain_products(products)

    def get_orders_in_domain_format(
        self, previous_days: int = 1, date_range: str = None, exchange_rates=None
    ) -> list[Order]:
        """Returns orders in a domain format."""
        date_from, date_to = self.resolve_date_range(
            previous_days=previous_days, date_range=date_range
        )
        print(f"Date from {date_from} to {date_to}")
        
        orders = self.get_orders(date_from=date_from, date_to=date_to)
        return self._to_domain_orders(orders, exchange_rates=exchange_rates)
    

    @abstractmethod
    def get_order_sources(self): ...

    @abstractmethod
    def get_orders(self): ...

    @abstractmethod
    def get_marketplaces(self): ...
    
    @abstractmethod
    def get_products(self): ...
    
    @abstractmethod
    def _to_simplified_orders(self): ...
    
    @abstractmethod
    def _to_domain_orders(self): ...
    
    @abstractmethod
    def _to_domain_products(self): ...
    
    @abstractmethod
    def _to_domain_marketplaces(self): ...
    
    
    def get_orders_from_xml(self, directory, date_from=None):
        import xml.etree.ElementTree as ET
        import glob
        import os
        import pytz
        from src.utils import convert_to_pln, code_to_country
        from src.clients.exchange_rates import ExchangeRateNbpApi
        """
        Loads orders from all XML files in the directory.
        Returns a list of dicts with only the required fields:
        - order_id
        - order_status_id (set to None or a default, as not present in XML)
        - date_add (epoch int)
        - order_source (from platform_account_name)
        - order_source_id (from external_shop_id)
        - payment_done (from invoice price_brutto)
        - delivery_price (set to 0 or parse if available)
        - currency (from invoice)
        - products (list with price_brutto and quantity=1 for each row)
        """
        domain_orders = []
        exchange_rates_api = ExchangeRateNbpApi()
        _exchange_rates_cache = {}
        
        def get_exchange_rates_for_date_cached(exchange_rates_api, date: str):
            """
            Returns exchange rates for a given date, using a simple in-class cache.
            """
            cache_key = date
            if cache_key in _exchange_rates_cache:
                return _exchange_rates_cache[cache_key]
            rates = exchange_rates_api.get_exchange_rates_for_date(date=date)
            _exchange_rates_cache[cache_key] = rates
            return rates
        
        for file in glob.glob(os.path.join(directory, "*.xml")):
            tree = ET.parse(file)
            root = tree.getroot()
            for order in root.findall('order'):
                # Get order_id
                order_id = order.findtext('order_id', default='')
                date_add = order.findtext('date_add', default='')
                delivery_type = order.findtext('delivery_type', default='')
                currency = order.findtext('currency', default='')
                client_city = order.findtext('client_city', default='')
                address_country_code = order.findtext('address_country_code', default='')
                if order.find('invoices/invoice') is None:
                    continue

                # Get platform_account_name as order_source
                source_name = order.findtext('platform_account_name', default='').strip()

                # Get external_shop_id as order_source_id
                source_type = order.findtext('platform_account', default='').strip()

                # Get delivery_price (if available)
                delivery_price = 0.0
                if order.find('delivery_price') is not None:
                    try:
                        delivery_price = float(order.find('delivery_price').text)
                    except Exception:
                        delivery_price = 0.0
                
                date_add_naive = datetime.strptime(date_add.strip(), "%d.%m.%Y %H:%M:%S")
                timezone = self.timezone
                date_add = timezone.localize(date_add_naive)
            
                date_curr = date_add.strftime("%Y-%m-%d")
                exchange_rates = get_exchange_rates_for_date_cached(exchange_rates_api, date_curr)
                

                # Filter by date_from if set
                if date_from is not None and date_add is not None and date_add < date_from:
                    continue
                # print(f"Processing order {order_id} date_add {date_add} date_from {date_from}")

                # Products: one for each <row> (simulate Baselinker API structure)
                products = []
                order_items = []
                rows = order.find('rows')

                for row in rows.findall('row'):
                    price_brutto = row.findtext('item_price_brutto')
                    quantity = row.findtext('quantity')
                    products_sku = row.findtext('products_sku')
                    name = row.findtext('name')
                    try:
                        price_brutto = float(price_brutto)
                    except Exception:
                        price_brutto = 0.0
                    products.append({
                        'price_brutto': price_brutto,
                        'quantity': int(quantity) 
                    })
                    order_items.append(
                        OrderItem(
                            sku=products_sku,
                            name=name,
                            price=price_brutto,
                            price_pln=convert_to_pln(price_brutto, currency, exchange_rates),
                            quantity=int(quantity),
                            # tax_rate= add
                        )
                    )

                payment_done = sum(p['price_brutto'] * p['quantity'] for p in products) + delivery_price
                # Compose order dict                
                domain_orders.append(
                        Order(
                            external_id=order_id,
                            total_gross_original=payment_done,
                            total_gross_pln=convert_to_pln(payment_done, currency, exchange_rates),
                            delivery_cost_original=delivery_price,
                            delivery_cost_pln=convert_to_pln(delivery_price, currency, exchange_rates),
                            delivery_method=delivery_type,
                            currency=currency,
                            status="archival_data",
                            country=code_to_country(address_country_code),
                            city=client_city,
                            created_at=date_add,
                            marketplace_extid=str(source_name),
                            marketplace_name=source_name,
                            platform_origin=self.platform_origin,
                            marketplace_type=source_type,
                            items=order_items,
                        )
                    )
        return domain_orders