import datetime
import pycountry

def convert_to_pln(row, exchange_rates):
    if row["currency"] in exchange_rates:
        return (
            row["total_net_payment_in_default_currency"]
            * exchange_rates[row["currency"]]
        )
    return row["total_net_payment_in_default_currency"]

def code_to_country(code: str) -> str | None:
    if not code:
        return None
    code = code.upper()
    if len(code) == 2:
        rec = pycountry.countries.get(alpha_2=code)
    elif len(code) == 3:
        rec = pycountry.countries.get(alpha_3=code)
    else:
        rec = None
    return rec.name if rec else None


def get_summary_string(df_sell, rename_dict):
    result = []
    result.append("-" * 25)
    for index, row in df_sell.iterrows():
        line = f"{rename_dict.get(index, index):<12} {'(' + str(row['order_count']) + ')':<6} {row['total_net_payment_pln']:>7,.0f}  PLN".replace(
            ",", " "
        )
        result.append(line)
    result.append("-" * 25)
    total_line = f"{'Razem ':<12} {'(' + str(df_sell['order_count'].sum()) + ')':<6} {df_sell['total_net_payment_pln'].sum():>7,.0f}  PLN".replace(
        ",", " "
    )
    result.append(total_line)
    return "\n".join(result)


def get_summary_table(df_sell, rename_dict):
    summary = []
    for index, row in df_sell.iterrows():
        summary.append(
            {
                "Marketplace": rename_dict.get(index, index),
                "Order Count": int(row["order_count"]),
                "Total Net Payment PLN": f"{row['total_net_payment_pln']:,.0f}".replace(
                    ",", " "
                ),
            }
        )
    summary.append(
        {
            "Marketplace": "Razem",
            "Order Count": int(df_sell["order_count"].sum()),
            "Total Net Payment PLN": f"{df_sell['total_net_payment_pln'].sum():,.0f}".replace(
                ",", " "
            ),
        }
    )
    return summary


def get_summary_table_simple(df_sell, rename_dict):
    summary = []
    for index, row in df_sell.iterrows():
        summary.append(
            {
                "marketplace": rename_dict.get(index, index),
                "orders_count": int(row["order_count"]),
                "revenue": int(row['total_net_payment_pln'])
            }
        )
    return summary


def generate_markdown_table(summary):
    headers = ["Marketplace", "Order Count", "Total Net Payment PLN"]
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["-" * len(h) for h in headers]) + " |"
    rows = []
    for entry in summary:
        row = f"| {entry['Marketplace']} | {entry['Order Count']} | {entry['Total Net Payment PLN']} PLN |"
        rows.append(row)
    return "\n".join([header_line, separator_line] + rows)


def generate_html_email(summary_table):
    html = []
    html.append("<html>")
    html.append("<head>")
    html.append("<style>")
    html.append("table { border-collapse: collapse; width: 100%; }")
    html.append("th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; }")
    html.append("th { background-color: #f2f2f2; }")
    html.append("</style>")
    html.append("</head>")
    html.append("<body>")
    html.append("<h2>Daily Sell Report</h2>")
    html.append("<table>")
    # Header row:
    headers = ["Marketplace", "Order Count", "Total Net Payment PLN"]
    html.append("<tr>")
    for h in headers:
        html.append(f"<th>{h}</th>")
    html.append("</tr>")
    # Data rows:
    for entry in summary_table:
        html.append("<tr>")
        html.append(f"<td>{entry['Marketplace']}</td>")
        html.append(f"<td>{entry['Order Count']}</td>")
        html.append(f"<td>{entry['Total Net Payment PLN']} PLN</td>")
        html.append("</tr>")
    html.append("</table>")
    html.append("</body>")
    html.append("</html>")
    return "\n".join(html)


def get_date_range(previous_days):
    return f"{(datetime.date.today() - datetime.timedelta(days=previous_days)).strftime('%d.%m.%Y')}-{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%d.%m.%Y')}"
