import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from selenium import webdriver
from selenium.webdriver.common.by import By
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def create_webdriver():
    desired_cap = {
        'os': 'Windows',
        'os_version': '10',
        'browser': 'Chrome',
        'browser_version': 'latest',
        'name': 'CFA Scraper',
        'build': 'Airflow-Selenium-Remote'
    }
    return webdriver.Remote(
        command_executor=f"http://{os.getenv('BROWSERSTACK_USERNAME')}:{os.getenv('BROWSERSTACK_ACCESS_KEY')}@hub-cloud.browserstack.com/wd/hub",
        desired_capabilities=desired_cap
    )

# Snowflake connection and inserting data function
def insert_into_snowflake(book_data):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cursor = conn.cursor()

    # Insert each book's data into Snowflake
    for book in book_data:
        cursor.execute(f"""
            INSERT INTO books (title, description, image_link, pdf_link)
            VALUES ('{book["title"]}', '{book["description"]}', '{book["image_link"]}', '{book["pdf_link"]}')
        """)
    
    conn.commit()
    cursor.close()
    conn.close()

# Function to scrape titles, descriptions, and store them with links in Snowflake
def scrape_and_store_metadata_in_snowflake():
    driver = create_webdriver()
    book_data = []

    cfa_url = "https://rpc.cfainstitute.org/en/research-foundation/publications"
    driver.get(cfa_url)

    # Scrape titles and descriptions
    titles = driver.find_elements(By.CSS_SELECTOR, "h3.SearchResults-title a")
    descriptions = driver.find_elements(By.CSS_SELECTOR, "p.SearchResults-description")

    s3_base_url = f'https://{S3_BUCKET_NAME}.s3.amazonaws.com/'

    for i in range(len(titles)):
        title = titles[i].text
        description = descriptions[i].text
        book_link = titles[i].get_attribute("href")

        # Navigate to book page to get image and pdf filenames
        driver.get(book_link)
        image_url = driver.find_element(By.CSS_SELECTOR, "img.book-cover").get_attribute("src")
        pdf_url = driver.find_element(By.LINK_TEXT, "Download PDF").get_attribute("href")

        # Extract filenames from URLs
        image_filename = image_url.split("/")[-1]
        pdf_filename = pdf_url.split("/")[-1]

        # Prepare book data to be stored in Snowflake
        book_data.append({
            "title": title,
            "description": description,
            "image_link": f'{s3_base_url}images/{image_filename}',
            "pdf_link": f'{s3_base_url}pdfs/{pdf_filename}'
        })

        driver.back()

    driver.quit()

    # Store the book data in Snowflake
    insert_into_snowflake(book_data)

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'scrape_metadata_and_store_in_snowflake',
    default_args=default_args,
    description='Scrape book titles, descriptions and store with image and PDF links in Snowflake',
    schedule_interval=None,
)

scrape_and_store_task = PythonOperator(
    task_id='scrape_and_store_metadata_in_snowflake',
    python_callable=scrape_and_store_metadata_in_snowflake,
    dag=dag,
)