import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import psycopg2
import re

st.set_page_config(page_title="Wikipedia Tabular Data Scraper",
                   layout="centered")

st.title('Wikipedia Tabular Data Scraper')
st.link_button(label='Mekhma Tamang',url='https://www.linkedin.com/in/mekhma-tamang/', icon=":material/link:",type='tertiary', help='Visit my LinkedIn')

wiki_url = st.text_input(label='Input Wikipedia URL below')

if "tables" not in st.session_state:
    st.session_state.update({
        "tables": [],
        "table_headings": {},
        "selected_df": None,
        "final_df": None,
        "table_title": None,
        "final_csv": None,
        "download_filename": None,
        "username": None,
        "password": None,
        "table_created": False,
        "form_submitted": False
    })

def fetch_tables(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    tables = [
        table for table in soup.find_all('table')
        if table.has_attr('class') and any('wikitable' in cls for cls in table['class'])
    ]


    table_headings = {}
    current_heading = None

    for tag in soup.find_all(['h2', 'h3', 'table']):
        if tag.name in ['h2', 'h3']:
            current_heading = tag.get_text(strip=True)
        elif tag.name == 'table' and tag in tables:
            table_headings[tag] = current_heading or f"Table {len(table_headings) + 1}"

    return tables, table_headings

if st.button('Fetch Data', type='secondary', icon=":material/query_stats:"):
    if not wiki_url.startswith("http"):
        st.error("Please enter a valid Wikipedia URL.")
    else:
        tables, table_headings = fetch_tables(wiki_url)
        st.session_state.tables = tables
        st.session_state.table_headings = table_headings

st.divider()
col1, col2 = st.columns(2)

with col1:
    if st.session_state.tables:
        for i, table in enumerate(st.session_state.tables):
            table_title = st.session_state.table_headings.get(table, f"Table {i+1}")

            if st.button(f"{i+1} : {table_title}", key=f'button_{i}', type="tertiary"):
                headers = [th.text.strip() for th in table.find_all('th')]

                df = pd.DataFrame(columns=headers)

                for row in table.find_all('tr')[1:]:
                    row_data = []
                    cols = row.find_all('td')

                    for i, col in enumerate(cols):
                        header = headers[i] if i < len(headers) else ""

                        # Check if the column header is "Image" or "Website"
                        if header in ["Image", "Website"]:
                            a_tag = col.find('a')
                            href = (a_tag['href'].strip() if a_tag and a_tag.has_attr('href') else '')
                            if header == 'Image' and href:
                                row_data.append(f'https://en.wikipedia.org{href}')
                        else:
                            row_data.append(col.text.strip())

                    row_data.extend([''] * (len(headers) - len(row_data)))
                    df.loc[len(df)] = row_data[:len(headers)]
                    
                

                st.session_state.selected_df = df.set_index(df.columns[0]).head(5)
                st.session_state.final_df = df
                st.session_state.table_title = table_title
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                st.session_state.final_csv = csv_buffer.getvalue()
                st.session_state.download_filename = f"{table_title.replace(' ', '_')}.csv"

with col2:
    if st.session_state.selected_df is not None:
        st.subheader(st.session_state.table_title)
        st.dataframe(st.session_state.selected_df)

        col3, col4 = st.columns(2)

        with col3:
            st.download_button(
                label="Download CSV",
                data=st.session_state.final_csv,
                file_name=st.session_state.download_filename,
                mime="text/csv"
            )

        with col4:
            if st.button(label='PostgreSQL', help='Insert this data into PostgreSQL'):
                st.session_state.show_db_form = True

if st.session_state.get("show_db_form", False):
    with st.form('PostgreSQL'):
        host = st.text_input('Host', value='localhost')
        username = st.text_input('Username')
        password = st.text_input('Password', type='password')
        submitted = st.form_submit_button('Submit')

        if submitted and host and username and password:
            st.session_state.username = username
            st.session_state.password = password
            st.session_state.form_submitted = True


            def connect():
                conn = psycopg2.connect(
                    host="localhost",
                    dbname="postgres", 
                    user=st.session_state.username,
                    password=st.session_state.password
                )
                conn.set_session(autocommit=True)
                cur = conn.cursor()

                cur.execute("SELECT 1 FROM pg_database WHERE datname='tables_from_wikipedia'")
                exists = cur.fetchone()

                if not exists:
                    cur.execute("CREATE DATABASE tables_from_wikipedia")

                cur.close()
                conn.close()

                conn = psycopg2.connect(
                    host="localhost",
                    dbname="tables_from_wikipedia",
                    user=st.session_state.username,
                    password=st.session_state.password
                )
                conn.set_session(autocommit=True)
                cur = conn.cursor()

                return cur, conn


            cur, conn = connect()

            conn = psycopg2.connect(f"host= localhost dbname=tables_from_wikipedia user={username} password={password}")
            cur = conn.cursor()

            raw_table_name = st.session_state.table_title
            table_name =  re.sub(r'[^a-zA-Z0-9_]', '_', raw_table_name)
            cur.execute("SELECT to_regclass(%s)", (table_name,))
            table_exists = cur.fetchone()[0]

            if table_exists:
                st.warning(f"Table '{table_name}' already exists!")
            else:
                final_df = st.session_state.final_df
                final_df_col = final_df.columns
                

                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name}("
                create_table_query += ", ".join(
                    [f'"{col}" TEXT' for col in final_df_col]
                ) + ");"

                insert_query = f"INSERT INTO {table_name} ({', '.join([f'\"{col}\"' for col in final_df_col])}) VALUES "
                placeholders = "(" + ", ".join(["%s"] * len(final_df.columns)) + ")"  # (%s, %s, %s, ...)
                values = [tuple(row) for row in final_df.itertuples(index=False, name=None)]

                cur.execute(create_table_query)
                conn.commit()
                cur.executemany(insert_query + placeholders,values)
                conn.commit()
                st.success(f'{table_name} created and data inserted successfully!')
            cur.close()
            conn.close()

if st.session_state.get("form_submitted", False):
    st.success('Credentials saved successfully!')

if st.session_state.get("table_created", False):
    st.success('Table created successfully!')
