import datetime
import json
import tempfile

import pytz
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import Secret
from prefect.futures import wait
from prefect.variables import Variable
from prefect_email import EmailServerCredentials, email_send_message
from prefect_gcp import GcpCredentials
from prefect_slack import SlackCredentials
from prefect_slack.messages import send_chat_message

from src.config import settings, update_settings
from src.clients.apilo import ApiloClient
from src.clients.baselinker import BaselinkerClient
from src.clients.exchange_rates import (
    ExchangeRateApiException,
    ExchangeRateNbpApi,
    ExchangeRateRapidApi,
)
from src.utils import (
    chunked_by_chunk_size,
    chunked_by_num_chunks,
    convert_to_pln_row,
    generate_html_email,
    generate_markdown_table,
    get_date_range,
    get_summary_string,
    get_summary_table,
    get_summary_table_simple,
)


def initialize_db_config():
    """Initialize database configuration from Prefect secrets."""
    try:
        db_url = Secret.load("psql-db-url").get()
        update_settings(POSTGRES_DB_URI=db_url)
    except ValueError as e:
        logger = get_run_logger()
        logger.warning(
            f"Could not load 'psql-db-url' from secrets: {e}. Using default: {settings.POSTGRES_DB_URI}"
        )


def get_apilo_client() -> ApiloClient:
    APILO_CLIENT_ID = Secret.load("apilo-client-id").get()
    APILO_CLIENT_SECRET = Secret.load("apilo-client-secret").get()
    APILO_AUTH_CODE = Secret.load("apilo-auth-code").get()
    APILO_TOKEN = Secret.load("apilo-token", validate=False).get()
    APILO_REFRESH_TOKEN = Secret.load("apilo-refresh-token", validate=False).get()
    APILO_URL = Secret.load("apilo-url").get()
    APILO_ORDER_STATUS_IDS_TO_IGNORE = Variable.get("apilo-order-status-ids-to-ignore")
    MARKETPLACE_RENAME_MAP = Variable.get("marketplace-rename-map", default={})
    TIMEZONE_PYTZ_STR = Variable.get("timezone-pytz-str", default="Europe/Warsaw")
    TIMEZONE = pytz.timezone(TIMEZONE_PYTZ_STR)

    return ApiloClient(
        client_id=APILO_CLIENT_ID,
        client_secret=APILO_CLIENT_SECRET,
        auth_code=APILO_AUTH_CODE,
        url=APILO_URL,
        token=APILO_TOKEN,
        refresh_token=APILO_REFRESH_TOKEN,
        timezone=TIMEZONE,
        order_status_ids_to_ignore=APILO_ORDER_STATUS_IDS_TO_IGNORE,
        marketplace_rename_map=MARKETPLACE_RENAME_MAP,
    )


def update_apilo_secrets(apilo_client: ApiloClient):
    Secret(value=apilo_client.token).save("apilo-token", overwrite=True)
    Secret(value=apilo_client.refresh_token).save("apilo-refresh-token", overwrite=True)


def get_baselinker_client() -> BaselinkerClient:
    BASELINKER_TOKEN = Secret.load("baselinker-token").get()
    BASELINKER_ORDER_STATUS_IDS_TO_IGNORE = Variable.get(
        "baselinker-order-status-ids-to-ignore"
    )
    MARKETPLACE_RENAME_MAP = Variable.get("marketplace-rename-map", default={})
    TIMEZONE_PYTZ_STR = Variable.get("timezone-pytz-str", default="Europe/Warsaw")
    TIMEZONE = pytz.timezone(TIMEZONE_PYTZ_STR)

    return BaselinkerClient(
        token=BASELINKER_TOKEN,
        timezone=TIMEZONE,
        order_status_ids_to_ignore=BASELINKER_ORDER_STATUS_IDS_TO_IGNORE,
        marketplace_rename_map=MARKETPLACE_RENAME_MAP,
    )


@task(retries=10, retry_delay_seconds=5, log_prints=True)
def get_exchange_rates_rapidapi():
    logger = get_run_logger()
    RAPIDAPI_KEY = Secret.load("rapidapi-key").get()
    RAPIDAPI_HOST = Secret.load("rapidapi-host").get()
    ex_rate_rapidapi = ExchangeRateRapidApi(api_key=RAPIDAPI_KEY, host=RAPIDAPI_HOST)
    try:
        exchange_rates = {
            "CZK": ex_rate_rapidapi.convert_currency(1, "CZK", "PLN"),
            "EUR": ex_rate_rapidapi.convert_currency(1, "EUR", "PLN"),
            "HUF": ex_rate_rapidapi.convert_currency(1, "HUF", "PLN"),
            "RON": ex_rate_rapidapi.convert_currency(1, "RON", "PLN"),
        }
    except (ExchangeRateApiException, Exception) as e:
        logger.error(f"Error getting exchange rates: {e}. Using default values.")
        logger.info(e)
        exchange_rates = {
            "CZK": 0.17,  # 1 CZK to PLN
            "EUR": 4.27,  # 1 EUR to PLN
            "HUF": 0.011,  # 1 HUF to PLN
            "RON": 0.84,  # 1 RON to PLN
        }
    return exchange_rates


@task(retries=10, retry_delay_seconds=5, log_prints=True)
def get_exchange_rates_nbp():
    return ExchangeRateNbpApi().get_exchange_rates(to_currencies="CZK,EUR,HUF,RON")


@task(log_prints=True)
def fetch_apilo_sell_statistics(previous_days=1, exchange_rates=None):
    apilo_client = get_apilo_client()
    df_sell, df_orders = apilo_client.get_sell_statistics_dataframe(
        conversion_rates=exchange_rates, previous_days=previous_days
    )
    update_apilo_secrets(apilo_client)
    return df_sell


@task(log_prints=True)
def fetch_apilo_orders(previous_days=1, exchange_rates=None):
    apilo_client = get_apilo_client()
    orders = apilo_client.get_orders_in_domain_format(
        previous_days=previous_days, exchange_rates=exchange_rates
    )
    update_apilo_secrets(apilo_client)
    return orders


@task(log_prints=True)
def fetch_apilo_products():
    apilo_client = get_apilo_client()
    products = apilo_client.get_products_in_domain_format()
    update_apilo_secrets(apilo_client)
    return products


@task(log_prints=True)
def fetch_apilo_marketplaces():
    apilo_client = get_apilo_client()
    marketplaces = apilo_client.get_marketplaces_in_domain_format()
    update_apilo_secrets(apilo_client)
    return marketplaces


@task(log_prints=True)
def fetch_baselinker_sell_statistics(previous_days=1, exchange_rates=None):
    baselinker_client = get_baselinker_client()
    df_sell, df_orders = baselinker_client.get_sell_statistics_dataframe(
        conversion_rates=exchange_rates, previous_days=previous_days
    )
    return df_sell


@task(log_prints=True)
def fetch_baselinker_orders(previous_days=1, exchange_rates=None):
    baselinker_client = get_baselinker_client()
    orders = baselinker_client.get_orders_in_domain_format(
        previous_days=previous_days, exchange_rates=exchange_rates
    )
    return orders


@task(log_prints=True)
def fetch_baselinker_products():
    baselinker_client = get_baselinker_client()
    products = baselinker_client.get_products_in_domain_format()
    return products


@task(log_prints=True)
def fetch_baselinker_marketplaces():
    baselinker_client = get_baselinker_client()
    marketplaces = baselinker_client.get_marketplaces_in_domain_format()
    return marketplaces


@task(log_prints=True)
def send_email(subject, body):
    email_server_credentials = EmailServerCredentials.load("gmail-app-pass")
    email_addresses = Variable.get("emails-to-send")
    for email_address in email_addresses:
        _ = email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject=subject,
            msg=body,
            email_to=email_address,
        )


@task(log_prints=True)
def send_slack_message(message):
    slack_credentials_block = SlackCredentials.load("slack-oauth-token")
    channel = Variable.get("slack-channel")
    send_chat_message.submit(
        slack_credentials=slack_credentials_block, channel=channel, text=message
    )


@task(log_prints=True)
def append_to_sheets_db(daily_sell_report, date: datetime.date):
    import gspread
    from google.oauth2.service_account import Credentials

    SHEETS_CRED = GcpCredentials.load("sheets-service-account")
    SHEET_ID = Variable.get("sheet-id")
    WORKSHEET_NAME = Variable.get("worksheet-name", "Dane")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        SHEETS_CRED.service_account_info.get_secret_value(), scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)
    for row in daily_sell_report:
        marketplace = row["marketplace"]
        orders = int(row["orders_count"])
        revenue = int(row["revenue"])
        ws.append_row(
            [date.isoformat(), marketplace, orders, revenue],
            value_input_option="USER_ENTERED",
        )


@task
def create_orders_batch(order_domain_dicts: list[dict]):
    initialize_db_config()
    from src.db.operations import bulk_upsert_orders_parallel
    bulk_upsert_orders_parallel(order_domain_dicts)


@task
def s3_download_file(key: str, bucket: str, endpoint_url: str = None):
    import boto3

    S3_ACCESS_KEY_ID = Secret.load("s3-bucket-access-key-id").get()
    S3_SECRET_ACCESS_KEY = Secret.load("s3-bucket-secret-access-key").get()

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    s3.download_fileobj(bucket, key, tmp_file)
    tmp_file.close()
    return tmp_file.name


@flow(flow_run_name="Daily Sell Report: previous_days={previous_days}", log_prints=True)
def get_sell_report(
    previous_days: int = 1,
    slack: bool = False,
    email: bool = False,
    sheets: bool = False,
):
    import pandas as pd

    MARKETPLACE_RENAME_MAP = Variable.get("marketplace-rename-map", default={})

    logger = get_run_logger()
    exchange_rates = get_exchange_rates_nbp.submit()
    df_sell_apilo = fetch_apilo_sell_statistics.submit(
        previous_days=previous_days, exchange_rates=exchange_rates
    )
    df_sell_base = fetch_baselinker_sell_statistics.submit(
        previous_days=previous_days, exchange_rates=exchange_rates
    )

    df_sell = pd.concat([df_sell_apilo.result(), df_sell_base.result()])
    df_sell["total_net_payment_pln"] = df_sell.apply(
        convert_to_pln_row, axis=1, exchange_rates=exchange_rates.result()
    )
    summary = get_summary_string(df_sell, MARKETPLACE_RENAME_MAP)
    date_range = get_date_range(previous_days)
    summary = f"{date_range}\n{summary}"
    logger.info(summary)
    summary_table = get_summary_table(df_sell, MARKETPLACE_RENAME_MAP)
    mkdn_table = generate_markdown_table(summary_table)
    create_markdown_artifact(
        key="daily-sell-report",
        markdown=mkdn_table,
        description=f"Daily sell statistics from Apilo & Baselinker. Date range: {date_range}, without Cancelled orders",
    )
    if email is True:
        send_email(
            subject=f"Daily Sell Report: {date_range}",
            body=generate_html_email(summary_table),
        )
    if slack is True:
        send_slack_message(
            f"Dzienny raport sprzedaży: {date_range}\n" + "```" + summary + "```"
        )
    if sheets is True:
        if previous_days == 1:
            date = datetime.date.today() - datetime.timedelta(days=previous_days)
            summary_table_simple = get_summary_table_simple(
                df_sell, MARKETPLACE_RENAME_MAP
            )
            append_to_sheets_db(summary_table_simple, date)


@flow(flow_run_name="Refresh Apilo Token", log_prints=True)
def refresh_apilo_token():
    logger = get_run_logger()
    apilo_client = get_apilo_client()
    apilo_client.refresh_access_token()
    Secret(value=apilo_client.token).save("apilo-token", overwrite=True)
    Secret(value=apilo_client.refresh_token).save("apilo-refresh-token", overwrite=True)
    logger.info("Apilo token refreshed successfully.")


@flow(flow_run_name="Display Apilo Token", log_prints=True)
def get_apilo_token_secret():
    logger = get_run_logger()
    APILO_TOKEN = Secret.load("apilo-token", validate=False).get()
    logger.info(f"Apilo token: {APILO_TOKEN}")


@flow(flow_run_name="debug-prefect-version", log_prints=True)
def debug_prefect_version():
    import pathlib
    import prefect
    print(
        "Running Prefect",
        prefect.__version__,
        "from",
        pathlib.Path(prefect.__file__).parent,
    )


@flow(flow_run_name="DB: Sync Products", log_prints=True)
def db_sync_products():
    logger = get_run_logger()
    initialize_db_config()
    from src.db.operations import bulk_upsert_products

    apilo_products = fetch_apilo_products.submit()
    baselinker_products = fetch_baselinker_products.submit()

    products_dict = {
        **baselinker_products.result(),
        **apilo_products.result(),
    }  # Apilo products take precedence
    products = products_dict.values()
    count = bulk_upsert_products(products)
    logger.info(f"Processed {count} products")


@flow(flow_run_name="DB: Sync Marketplaces", log_prints=True)
def db_sync_marketplaces():
    logger = get_run_logger()
    initialize_db_config()
    from src.db.operations import bulk_upsert_marketplaces

    apilo_marketplaces = fetch_apilo_marketplaces.submit()
    baselinker_marketplaces = fetch_baselinker_marketplaces.submit()

    marketplaces = apilo_marketplaces.result() + baselinker_marketplaces.result()
    count = bulk_upsert_marketplaces(marketplaces)
    logger.info(f"Processed {count} marketplaces")


@flow(flow_run_name="DB: Sync Offers: Apilo", log_prints=True)
def db_sync_offers_apilo():
    logger = get_run_logger()
    initialize_db_config()
    from src.db.operations import bulk_upsert_offers

    apilo_client = get_apilo_client()

    offers = apilo_client.get_offers_in_domain_format()
    update_apilo_secrets(apilo_client)
    
    logger.info(f"Total offers fetched: {len(offers)}")
    updated = bulk_upsert_offers(offers)
    logger.info(f"Updated {updated} offers from Apilo")


@flow(
    flow_run_name="DB: Collect Orders: previous_days={previous_days}", log_prints=True
)
def db_collect_orders(
    previous_days: int = 1, apilo: bool = True, baselinker: bool = True
):
    logger = get_run_logger()
    initialize_db_config()
    from src.db.operations import bulk_upsert_orders

    exchange_rates = get_exchange_rates_nbp.submit()
    orders_apilo = None
    orders_baselinker = None

    if apilo:
        orders_apilo = fetch_apilo_orders.submit(
            previous_days=previous_days, exchange_rates=exchange_rates
        )
    if baselinker:
        orders_baselinker = fetch_baselinker_orders.submit(
            previous_days=previous_days, exchange_rates=exchange_rates
        )

    orders = []
    if orders_apilo is not None:
        orders.extend(orders_apilo.result())
    if orders_baselinker is not None:
        orders.extend(orders_baselinker.result())

    logger.info(f"Total orders fetched: {len(orders)}")
    created = bulk_upsert_orders(orders)
    logger.info(f"Newly created orders: {created}")


@flow(
    flow_run_name="DB: Collect Orders parallel: previous_days={previous_days}",
    log_prints=True,
)
def db_collect_orders_parallel(
    previous_days: int = 1, apilo: bool = True, baselinker: bool = True
):
    logger = get_run_logger()
    initialize_db_config()
    exchange_rates = get_exchange_rates_nbp.submit()
    orders_apilo = None
    orders_baselinker = None

    if apilo:
        orders_apilo = fetch_apilo_orders.submit(
            previous_days=previous_days, exchange_rates=exchange_rates
        )
    if baselinker:
        orders_baselinker = fetch_baselinker_orders.submit(
            previous_days=previous_days, exchange_rates=exchange_rates
        )

    orders = []
    if orders_apilo is not None:
        orders.extend(orders_apilo.result())
    if orders_baselinker is not None:
        orders.extend(orders_baselinker.result())

    logger.info(f"Total orders fetched: {len(orders)}")
    order_dicts = [o.model_dump(mode="json") for o in orders]
    # batch_size = 500
    # batches = list(chunked_by_chunk_size(order_dicts, batch_size))
    batch_num = 20
    batches = list(chunked_by_num_chunks(order_dicts, batch_num))
    batch_orders = create_orders_batch.map(batches)
    wait(batch_orders)


@flow(flow_run_name="DB: Collect Stock History", log_prints=True)
def db_collect_stock_history(key: str):
    BUCKET_NAME = Variable.get("s3-bucket-name")
    ENDPOINT_URL = Variable.get("s3-bucket-endpoint-url")
    TIMEZONE_PYTZ_STR = Variable.get("timezone-pytz-str", default="Europe/Warsaw")
    TIMEZONE = pytz.timezone(TIMEZONE_PYTZ_STR)
    logger = get_run_logger()
    initialize_db_config()
    from src.db.operations import bulk_create_stock_history

    dt_now = datetime.datetime.now(TIMEZONE)

    stock_file = s3_download_file(
        key=key, bucket=BUCKET_NAME, endpoint_url=ENDPOINT_URL
    )

    with open(stock_file, "r", encoding="utf-8") as f:
        products = json.load(f)

    logger.info(f"Total products fetched: {len(products)}")
    bulk_create_stock_history(products, date=dt_now)


@flow(
    flow_run_name="DB: Collect Orders with Deps: previous_days={previous_days}",
    log_prints=True,
)
def db_collect_orders_with_deps(
    previous_days: int = 1, apilo: bool = True, baselinker: bool = True
):
    db_sync_marketplaces()
    db_sync_products()
    db_collect_orders(previous_days=previous_days, apilo=apilo, baselinker=baselinker)


if __name__ == "__main__":
    # db_collect_orders_with_deps()
    db_sync_offers_apilo()