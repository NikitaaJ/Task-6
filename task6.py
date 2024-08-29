import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
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

def scrape_reliance_data(session, symbol):
    search_url = f"https://www.screener.in/company/{symbol}/consolidated/"
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        print(f"{symbol} data retrieved successfully")
        soup = BeautifulSoup(search_response.content, 'html.parser')
        table1 = soup.find('section', {'id': 'profit-loss'})
        table = table1.find('table')
        headers = [th.text.strip() for th in table.find_all('th')]
        rows = table.find_all('tr')
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
            df.columns = ['Year'] + df.columns[1:].tolist()
            df = df.rename(columns={'Narration': 'Year', 'Year': 'year'})
        df_transposed = df.transpose().reset_index()
        df_transposed.rename(columns={'index': 'Narration'}, inplace=True)
        df_transposed = df_transposed.reset_index(drop=True)
        df_transposed.columns = [col.strip() for col in df_transposed.iloc[0]]  
        df_transposed = df_transposed[1:]  
        df_transposed = df_transposed.reset_index(drop=True)
        df_transposed.columns = [col if col else 'Unknown' for col in df_transposed.columns]  
        df_transposed = df_transposed.replace('', 0)  
        df_transposed = df_transposed.replace(np.nan, 0)  
        cleaned_columns = []
        for col in df_transposed.columns:
            cleaned_col = col.replace(' ', '_').replace('+', '').strip()
            cleaned_columns.append(cleaned_col)
        df_transposed.columns = cleaned_columns
        for col in df_transposed.columns[1:]:
            df_transposed[col] = df_transposed[col].apply(clean_data)
        print(df_transposed.columns)  # Print the column names
        df_transposed = df_transposed[df_transposed['year'] != 'TTM']  # Drop the TTM row
        print(df_transposed.head())
        return df_transposed
    else:
        print(f"Failed to retrieve {symbol} data")
        return None

def clean_data(value):
    if isinstance(value, str):
        value = value.replace("+", "").replace("%", "").replace(",", "").replace(" ", "").strip()
        if value.replace('.', '', 1).isdigit():
            try:
                return float(value)
            except ValueError:
                return 0.0  
        return value.replace(',', '')  
    return value

def save_to_postgres(df, table_name, db, user, password, host, port):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    try:
        for col in df.columns[1:]:
            df[col] = df[col].apply(clean_data)
        df = df.fillna(0)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data saved to Postgres")
    except SQLAlchemyError as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

def read_company_names_from_csv(file_path):
    try:
        df = pd.read_csv("C:\Users\Other User\Downloads\ind_nifty50list 2.csv")
        print("DataFrame loaded from CSV:")
                print(df.head())
        if 'Symbol' in df.columns and 'Company Name' in df.columns:
            company_symbols = df['Symbol'].unique()
            company_names = df['Company Name'].unique()
            print("\nCompany symbols extracted:")
            print(company_symbols)
            print("\nCompany names extracted:")
            print(company_names)
            return company_symbols, company_names
        else:
            print("Required columns not found in the CSV file.")
            return None, None
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None, None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table_name", default="company_data2")
    parser.add_argument("--db", default="Task6")
    parser.add_argument("--user", default="Nikita")
    parser.add_argument("--pw", default="Nikita06")
    parser.add_argument("--host", default="192.168.1.193")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--csv_file", required=True)
    args = parser.parse_args()
    session = login_to_screener(args.email, args.password)
    if session:
        company_symbols, company_names = read_company_names_from_csv(args.csv_file)
        if company_symbols is not None and company_names is not None:
            all_df = pd.DataFrame()
            for symbol, name in zip(company_symbols, company_names):
                df = scrape_reliance_data(session, symbol)
                if df is not None:
                    df['Company'] = name
                    all_df = pd.concat([all_df, df], ignore_index=True)
            save_to_postgres(all_df, args.table_name, args.db, args.user, args.pw, args.host, args.port)
