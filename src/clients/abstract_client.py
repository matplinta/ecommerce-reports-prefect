from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import pandas as pd

from src.domain.entities import Order


class AbstractClient(ABC):
    def __init__(self, timezone, order_status_ids_to_ignore=None, marketplace_rename_map=None):
        self.timezone = timezone
        self.order_status_ids_to_ignore = order_status_ids_to_ignore or []
        self.marketplace_rename_map = marketplace_rename_map or {}

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
    def _to_simplified_orders(self): ...
    
    @abstractmethod
    def _to_domain_orders(self): ...
    
    @abstractmethod
    def get_order_sources(self): ...

    @abstractmethod
    def get_orders(self): ...
