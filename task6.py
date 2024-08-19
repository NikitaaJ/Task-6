import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

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
        print(df.head())
        return df
    else:
        print("Failed to retrieve Reliance data")
        return None

def save_to_postgres(df, table_name, db, user, password, host, port):
    engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}", connect_args={'port': port})
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data saved to Postgres")
    except SQLAlchemyError as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

email = "nikita.jethani@godigitaltc.com"
password = "test@1233"
table_name = "financial_data"
db = "Task6"
user = "Nikita"
pw = "Nikita06"
host = "192.168.1.85"
port = "5432"

session = login_to_screener(email, password)
if session:
    df = scrape_reliance_data(session)
    if df is not None:
        save_to_postgres(df, table_name, db, user, pw, host, port)
