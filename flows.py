import datetime

import gspread
import pandas as pd
import pytz
from google.oauth2.service_account import Credentials
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.blocks.system import Secret
from prefect.variables import Variable
from prefect_email import EmailServerCredentials, email_send_message
from prefect_gcp import GcpCredentials
from prefect_slack import SlackCredentials
from prefect_slack.messages import send_chat_message
from pydantic import BaseModel

from src.api_exchange_rates import ExchangeRateNbpApi, ExchangeRateRapidApi, ExchangeRateApiException
from src.api_handlers import ApiloApi, BaselinkerApi
from src.utils import (
    convert_to_pln,
    generate_html_email,
    generate_markdown_table,
    get_date_range,
    get_summary_string,
    get_summary_table,
    get_summary_table_simple,
)

TIMEZONE = pytz.timezone("Europe/Warsaw")

BASELINKER_TOKEN = Secret.load("baselinker-token").get()
APILO_CLIENT_ID = Secret.load("apilo-client-id").get()
APILO_CLIENT_SECRET = Secret.load("apilo-client-secret").get()
APILO_AUTH_CODE = Secret.load("apilo-auth-code").get()
APILO_TOKEN = Secret.load("apilo-token", validate=False).get()
APILO_REFRESH_TOKEN = Secret.load("apilo-refresh-token", validate=False).get()
APILO_URL = Secret.load("apilo-url").get()
RAPIDAPI_KEY = Secret.load("rapidapi-key").get()
RAPIDAPI_HOST = Secret.load("rapidapi-host").get()
RENAME_DICT = Variable.get("rename-dict")


@task(retries=10, retry_delay_seconds=5, log_prints=True)
def get_exchange_rates_rapidapi():
    logger = get_run_logger()
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
def gather_apilo_sell_statistics(previous_days=1, exchange_rates=None):
    apilo_api_instance = ApiloApi(
        client_id=APILO_CLIENT_ID,
        client_secret=APILO_CLIENT_SECRET,
        auth_code=APILO_AUTH_CODE,
        url=APILO_URL,
        token=APILO_TOKEN,
        refresh_token=APILO_REFRESH_TOKEN,
    )
    df_sell, df_orders = apilo_api_instance.get_sell_statistics_dataframe(
        conversion_rates=exchange_rates, previous_days=previous_days
    )
    Secret(value=apilo_api_instance.token).save("apilo-token", overwrite=True)
    Secret(value=apilo_api_instance.refresh_token).save(
        "apilo-refresh-token", overwrite=True
    )
    return df_sell


@task(log_prints=True)
def gather_baselinker_sell_statistics(previous_days=1, exchange_rates=None):
    base_api_cz = BaselinkerApi(token=BASELINKER_TOKEN)
    df_sell, df_orders = base_api_cz.get_sell_statistics_dataframe(
        conversion_rates=exchange_rates, previous_days=previous_days
    )
    return df_sell


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
    sh  = gc.open_by_key(SHEET_ID)
    ws  = sh.worksheet(WORKSHEET_NAME)
    for row in daily_sell_report:
        marketplace = row["marketplace"]
        orders = int(row["orders_count"])
        revenue = int(row["revenue"])
        ws.append_row(
            [date.isoformat(), marketplace, orders, revenue],
            value_input_option="USER_ENTERED",
        )


class SellReportParams(BaseModel):
    previous_days: int = 1
    slack: bool = False
    email: bool = False
    sheets: bool = False


@flow(flow_run_name="Daily Sell Report: previous_days={previous_days}", log_prints=True)
def get_sell_report(previous_days: int=1, slack: bool=False, email: bool=False, sheets: bool=False):

    logger = get_run_logger()
    exchange_rates = get_exchange_rates_nbp.submit()
    df_sell_apilo = gather_apilo_sell_statistics.submit(
        previous_days=previous_days, exchange_rates=exchange_rates
    )
    df_sell_base = gather_baselinker_sell_statistics.submit(
        previous_days=previous_days, exchange_rates=exchange_rates
    )

    df_sell = pd.concat([df_sell_apilo.result(), df_sell_base.result()])
    df_sell["total_net_payment_pln"] = df_sell.apply(
        convert_to_pln, axis=1, exchange_rates=exchange_rates.result()
    )
    summary = get_summary_string(df_sell, RENAME_DICT)
    date_range = get_date_range(previous_days)
    summary = f"{date_range}\n{summary}"
    logger.info(summary)
    summary_table = get_summary_table(df_sell, RENAME_DICT)
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
            summary_table_simple = get_summary_table_simple(df_sell, RENAME_DICT)
            append_to_sheets_db(summary_table_simple, date)


@flow(flow_run_name="Refresh Apilo Token", log_prints=True)
def refresh_apilo_token():
    logger = get_run_logger()
    apilo_api_instance = ApiloApi(
        client_id=APILO_CLIENT_ID,
        client_secret=APILO_CLIENT_SECRET,
        auth_code=APILO_AUTH_CODE,
        url=APILO_URL,
        token=APILO_TOKEN,
        refresh_token=APILO_REFRESH_TOKEN,
    )
    apilo_api_instance.refresh_access_token()
    Secret(value=apilo_api_instance.token).save("apilo-token", overwrite=True)
    Secret(value=apilo_api_instance.refresh_token).save(
        "apilo-refresh-token", overwrite=True
    )
    logger.info("Apilo token refreshed successfully.")


@flow(flow_run_name="Display Apilo Token", log_prints=True)
def get_apilo_token_secret():
    logger = get_run_logger()
    logger.info(f"Apilo token: {APILO_TOKEN}")
    

@flow(flow_run_name="debug-prefect-version", log_prints=True)
def debug_prefect_version():
    import prefect, pathlib
    print("Running Prefect", prefect.__version__, "from", pathlib.Path(prefect.__file__).parent)



if __name__ == "__main__":
    get_sell_report()
