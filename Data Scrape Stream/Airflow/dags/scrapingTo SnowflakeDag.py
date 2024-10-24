import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

# Scraping function
def scrape_data():
    # Set up Selenium WebDriver
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    url = 'https://rpc.cfainstitute.org/en/research-foundation/publications?sort=%40officialz32xdate%20descending&f:SeriesContent=%5BResearch%20Foundation%5D'
    driver.get(url)

    publications = driver.find_elements_by_css_selector('.coveo-result-frame.coveoforsitecore-template')

    # Extract data
    scraped_data = []
    for pub in publications:
        title = pub.find_element_by_css_selector('h4.coveo-title').text
        summary = pub.find_element_by_css_selector('div.result-body').text
        image_url = pub.find_element_by_css_selector('img.coveo-result-image').get_attribute('src')
        pdf_url = pub.find_element_by_css_selector('a.CoveoResultLink').get_attribute('href')
        scraped_data.append((title, summary, image_url, pdf_url))

    driver.quit()

    # Save data to Snowflake
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    insert_query = "INSERT INTO publications (title, summary, image_url, pdf_url) VALUES (%s, %s, %s, %s)"
    for data in scraped_data:
        cursor.execute(insert_query, data)

    conn.commit()
    cursor.close()
    conn.close()

# Airflow DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'scrape_and_save_to_snowflake',
    default_args=default_args,
    description='Scrape data and save to Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 22),
    catchup=False,
) as dag:

    scrape_and_save_task = PythonOperator(
        task_id='scrape_data_and_save_to_snowflake',
        python_callable=scrape_data
    )