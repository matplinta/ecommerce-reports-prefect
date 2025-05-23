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
| rapidapi-key         | Your RapidAPI key                   |
| rapidapi-host        | Your RapidAPI host                  |

> ℹ️ **_NOTE:_**  For the first apilo token initializaiton, set `apilo-token` and `apilo-refresh-token` to "-1"

### Required Variable

| Key         | Description                                                    |
|-------------|----------------------------------------------------------------|
| rename-dict | A dictionary for renaming marketplace keys in the final report |

### Optional Secrets and Variables

#### Email via Gmail

- Secret `gmail-app-pass` of type `EmailServerCredentials` for Gmail account, containing App Password token,
- Variable `emails-to-send` which is a list of strings of e-mail addresses to which emails should be sent.

#### Slack

- Secret `slack-oauth-token` of type `SlackCredentials` for Slack App, Slack bot OAuth token,
- Variable `slack-channel` being a string with name of slack channel onto which the message should be sent. App slack bot should be added to the channel prior.

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

To deploy the script, for example on prefect-cloud: 
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

### **refresh_apilo_token**  
Refreshes the Apilo API token and updates the corresponding Prefect Secrets.  

### **get_apilo_token_secret**  
Displays the current Apilo token stored in the Prefect Secret.  
