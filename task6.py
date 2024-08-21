import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
import argparse
import numpy as np

def login_to_screener(email, password):
    session = requests.Session()
    login_url = "https://www.screener.in/login/?"
    login_page = session.get(login_url)
    soup = BeautifulSoup(login_page.content, 'html.parser')
    csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
    login_payload = {
        'username': email,
        'password': password,
        'csrfmiddlewaretoken': csrf_token
    }
    headers = {
        'Referer': login_url,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
    }
    response = session.post(login_url, data=login_payload, headers=headers)
    if response.url == "https://www.screener.in/dash/":
        print("Login successful")
        return session
    else:
        print("Login failed")
        return None

def scrape_reliance_data(session):
    search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        print("Reliance data retrieved successfully")
        soup = BeautifulSoup(search_response.content, 'html.parser')
        table1 = soup.find('section', {'id': 'profit-loss'})
        table = table1.find('table')
        headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
        rows = table.find_all('tr')
        print("Extracted Headers:", headers)
        row_data = []
        for row in rows[1:]:
            cols = row.find_all('td')
            cols = [col.text.strip() for col in cols]
            if len(cols) == len(headers):
                row_data.append(cols)
            else:
                print(f"Row data length mismatch: {cols}")
        df = pd.DataFrame(row_data, columns=headers)
        if not df.empty:
            df.columns = ['Narration'] + df.columns[1:].tolist()
        df = df.reset_index(drop=True)
        # Transpose the DataFrame
        df = df.transpose()
        # Handle missing values
        df = df.apply(pd.to_numeric, errors='coerce')
        print(df.head())
        return df
    else:
        print("Failed to retrieve Reliance data")
        return None

def save_to_postgres(df, table_name, db, user, password, host, port):
    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    try:
        # Specify data types for each column
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        print("Data saved to Postgres")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table_name", default="financial_data")
    parser.add_argument("--db", default="Task6")
    parser.add_argument("--user", default="Nikita")
    parser.add_argument("--pw", default="Nikita06")
    parser.add_argument("--host", default="192.168.1.85")
    parser.add_argument("--port", default="5432")
    args = parser.parse_args()
    session = login_to_screener(args.email, args.password)
    if session:
        df = scrape_reliance_data(session)
        if df is not None:
            save_to_postgres(df, args.table_name, args.db, args.user, args.pw, args.host, args.port)
