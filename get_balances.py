import requests
import csv

import lib_bq
import pandas as pd

# Replace with your YNAB access token and budget ID
YNAB_TOKEN = "YpxPH_8HxynRQXxgJkRrguo2Sd2t7t1NT-XxtWqT_ZM"
BUDGET_ID = "aca294de-e11a-4857-908f-bf19edaee302"
FILENAME = "data/account_balances.csv"
BQ_TABLE_NAME = "ynab_balances"

API_URL = f"https://api.ynab.com/v1/budgets/{BUDGET_ID}"
HEADERS = {"Authorization": f"Bearer {YNAB_TOKEN}"}

def fetch_account_balances():
    response = requests.get(f"{API_URL}/accounts", headers=HEADERS)
    response.raise_for_status()
    accounts = response.json()['data']['accounts']
    rows = []
    for account in accounts:
        rows.append({
            "id": account["id"],
            "name": account["name"],
            "balance": account["balance"] / 1000,
            "cleared_balance": account["cleared_balance"] / 1000,
            "uncleared_balance": account["uncleared_balance"] / 1000
        })
    return rows

def write_csv(rows):
    with open(FILENAME, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def write_bigquery(rows):
    df = pd.DataFrame(rows)
    lib_bq.write_to_bigquery(df, BQ_TABLE_NAME)

def main():
    rows = fetch_account_balances()
    write_csv(rows)
    write_bigquery(rows)

if __name__ == "__main__":
    main()
