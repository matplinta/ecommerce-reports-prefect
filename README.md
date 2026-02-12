# 📊 Ecommerce Reports Prefect

Generate daily ecommerce sell reports from Apilo and Baselinker APIs. The workflows are defined using Prefect to orchestrate tasks like fetching exchange rates, gathering statistics, and creating formatted report artifacts as well as writing to a postgres DB.

## Overview

- **Prefect Workflows:**  
  The project uses Prefect flows to manage tasks such as:
  - Retrieving exchange rates
  - Fetching sell statistics from Apilo and Baselinker
  - Converting currency values to PLN
  - Generating summary reports and artifacts
  - Writing data to postgres DB

- **APIs:**  
  It integrates with external APIs for ecommerce data from:
  - Apilo
  - Baselinker

## Prerequisites

Before running the flows, ensure you have properly defined the following Secrets and Variables within your Prefect environment:

### Required Secrets

| Key                  | Description                         |
|----------------------|-------------------------------------|
| baselinker-token     | Your Baselinker API token           |
| apilo-client-id      | Your Apilo client ID                |
| apilo-client-secret  | Your Apilo client secret            |
| apilo-auth-code      | Your Apilo authorization code       |
| apilo-token          | Your Apilo token                    |
| apilo-refresh-token  | Your Apilo refresh token            |
| apilo-url            | The base URL for the Apilo API      |

> ℹ️ **_NOTE:_**  For the first apilo token initializaiton, set `apilo-token` and `apilo-refresh-token` to "-1"

### Required Variable

| Key         | Description                                                    |
|-------------|----------------------------------------------------------------|
| marketplace-rename-map | A dictionary for renaming marketplace keys in the final report |

### Optional Secrets and Variables

#### Email via Gmail

- Secret `gmail-app-pass` of type `EmailServerCredentials` for Gmail account, containing App Password token,
- Variable `emails-to-send` which is a list of strings of e-mail addresses to which emails should be sent.

#### Slack

- Secret `slack-oauth-token` of type `SlackCredentials` for Slack App, Slack bot OAuth token,
- Variable `slack-channel` being a string with name of slack channel onto which the message should be sent. App slack bot should be added to the channel prior.

#### Google Sheets

- Secret `sheets-service-account` of type `GcpCredentials` for GCP service account credentials info,
- Variable `sheet-id` being a string with sheet-id from Google Sheets
- Variable `worksheet-name` [optional] - worksheet name, defaults to "Dane"

#### Postgres

- Secret `psql-db-url` containing db uri of the postgres database.

#### Misc

- Variable  `timezone-pytz-str` containing the pytz timezone string. Defaults to "Europe/Warsaw"
- Variable `baselinker-order-status-ids-to-ignore` containing a list of ids in integers of statuses which should be globally ignored during data collection from Baselinker
- Variable `apilo-order-status-ids-to-ignore` containing a list of ids in integers of statuses which should be globally ignored during data collection from Apilo

## Running and scheduling the flows

Please follow instructions for quickstart from prefect docs: https://docs.prefect.io/v3/get-started/quickstart


Execute the main flow locally by running the script via:
```
uv run flows.py
```
or 
```
python flows.py
```

## Prefect Deployments

This repository includes a `prefect.yaml` file that defines multiple Prefect deployments for the available flows, including scheduling and parameters.

### Deploying a Flow

To deploy a specific flow using the Prefect CLI, use the `--name` flag with the deployment name defined in `prefect.yaml`. For example, to deploy the daily sell reports flow, run:

```
uvx prefect deploy --name "daily sell reports"
```

This command will register the deployment with Prefect, making it available for execution and scheduling according to the configuration in `prefect.yaml`.

For more details, see the [Prefect documentation on deployments](https://docs.prefect.io/latest/concepts/deployments/).
To deploy the script, using a simpler approach, for example on prefect-cloud:

```
uvx prefect-cloud deploy flows.py:get_sell_report
```

## Available Flows

### **get_sell_report**  
Gathers sell statistics from Apilo and Baselinker APIs, converts currency values to PLN, generates summary reports, and creates report artifacts.  
**Parameters:**  
- `previous_days` (int): Number of previous days to include in the report  
- `slack` (bool): If true, sends the report to Slack  
- `email` (bool): If true, sends the report via email
- `sheets` (bool): If true, appends the report data to Google Sheets

### **refresh_apilo_token**  
Refreshes the Apilo API token and updates the corresponding Prefect Secrets.  

### **get_apilo_token_secret**  
Displays the current Apilo token stored in the Prefect Secret.

### **debug_prefect_version**  
Displays the current prefect version. Used only for debugging purposes.

### **db_sync_products**  
Synchronizes product data from external sources (Apilo and Baselinker) into the Postgres database. Performs upserts to ensure the database reflects the latest product information.  
**Parameters:**  
_None_

### **db_sync_marketplaces**  
Synchronizes marketplace data from Apilo and Baselinker APIs into the database, updating or inserting marketplace records as needed.  
**Parameters:**  
_None_

### **db_sync_offers_apilo**  
Fetches offer data from Apilo, processes it, and upserts offers into the database, ensuring offer records are current.  
**Parameters:**  
_None_

### **db_collect_orders**  
Collects order data from Apilo and Baselinker for a specified date range and writes the results to the database. Uses serial process approach.  
**Parameters:**  
- `previous_days` (int): Number of previous days to include  
- `apilo` (bool): If true, collects orders from Apilo  
- `baselinker` (bool): If true, collects orders from Baselinker

### **db_collect_orders_parallel**  
Collects orders from Apilo and Baselinker in parallel batches for improved performance, then writes the results to the database.  
**Parameters:**  
- `previous_days` (int): Number of previous days to include  
- `apilo` (bool): If true, collects orders from Apilo  
- `baselinker` (bool): If true, collects orders from Baselinker

### **db_collect_orders_with_deps**  
Collects orders along with all required dependencies (such as products and marketplaces), ensuring all related data is synchronized in the database. Made mainly for workaround purposes related to limited deployments in prefect cloud.  
**Parameters:**  
- `previous_days` (int): Number of previous days to include  
- `apilo` (bool): If true, collects orders from Apilo  
- `baselinker` (bool): If true, collects orders from Baselinker

### **db_collect_stock_history**  
Collects and stores historical stock levels for products, enabling tracking of inventory changes over time.  
**Parameters:**  
- `key` (str): S3 key for the stock file to process

# Migrations (alembic)
Change src/db/alembic.ini `sqlalchemy.url` value to the connection to db url 

OR

just change the **POSTGRES_DB_URI** in the `src/config.py` (or the top level setting in the .env file)

First, let's suppose you want to create migration in your local environment. 
If you have already a long standing DB with previous alembic_version specified, then you just need to run the following lines.

> Warning! 
> 
> If you created a new local DB from scratch (maybe using `just purge-local-db` followed by `just init-db`), then you first need to run `alembic stamp head` to let the alembic know that this is the newest version of the schemas. Only then you should make the model changes that you require in your code, and then run the following lines (creating the migraiton and actually applying it). 

To generate a new migration:
```
alembic revision --autogenerate -m "add unit_purchace_cost to Product"
```
Then to apply the migrations to your DB instance:
```
alembic upgrade head
```

# Local development

For local development, use provided preconfigured routines defined in justfile. 

If you wish to use local database, change the following line inside `flows.py` in `initialize_db_config`:
```python
db_url = Secret.load("psql-db-url").get()
```

So that the secret is not recognized from the cloud (so for example change "psql-db-url" to "xxxxx"). This will trigger the except statement to use the default value, which should be defined inside the .env file as: 
```
POSTGRES_DB_URI="postgresql+psycopg2://dev:secret@localhost:5432/shop"
``` 