# 📊 Ecommerce Reports Prefect

Generate daily ecommerce sell reports from Apilo and Baselinker APIs. The workflows are defined using Prefect to orchestrate tasks like fetching exchange rates, gathering statistics, and creating formatted report artifacts.

## Overview

- **Prefect Workflows:**  
  The project uses Prefect flows to manage tasks such as:
  - Retrieving exchange rates
  - Fetching sell statistics from Apilo and Baselinker
  - Converting currency values to PLN
  - Generating summary reports and artifacts

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
| rename-dict | A dictionary for renaming marketplace keys in the final report |

### Optional Secrets and Variables

#### RapidAPI

- Secret `rapidapi-key` (optional): Your RapidAPI key  
- Secret `rapidapi-host` (optional): Your RapidAPI host  

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

# Migrations (alembic)
Change src/db/alembic.ini `sqlalchemy.url` value to the connection to db url
```
alembic revision --autogenerate -m "add unit_purchace_cost to Product"
```

```
alembic upgrade head
```