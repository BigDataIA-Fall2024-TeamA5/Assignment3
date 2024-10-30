import os
import requests
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Configure remote Selenium WebDriver
def create_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")

    logger.info("Creating Selenium WebDriver with remote URL...")
    driver = webdriver.Remote(
        command_executor=f"http://selenium_remote:4444/wd/hub",
        options=chrome_options
    )
    logger.info("WebDriver created successfully!")
    return driver

# Function to scrape CFA book page and store image/pdf in S3
def scrape_and_upload_to_s3():
    logger.info("Starting the scraping task.")
    driver = create_webdriver()

    try:
        # URL to CFA research foundation publications
        cfa_url = "https://rpc.cfainstitute.org/en/research-foundation/publications"
        logger.info(f"Navigating to {cfa_url}")
        driver.get(cfa_url)

        # Find the book elements (titles)
        titles = driver.find_elements(By.CSS_SELECTOR, "h3.SearchResults-title a")
        logger.info(f"Found {len(titles)} book titles.")

        s3 = boto3.client('s3')

        # Loop through each book entry
        for index, title in enumerate(titles):
            book_link = title.get_attribute("href")
            logger.info(f"Processing book {index + 1}/{len(titles)}: {book_link}")
            driver.get(book_link)

            # Get image URL and PDF URL on the book detail page
            try:
                image_url = driver.find_element(By.CSS_SELECTOR, "img.book-cover").get_attribute("src")
                pdf_url = driver.find_element(By.LINK_TEXT, "Download PDF").get_attribute("href")
                logger.info(f"Found image: {image_url}, Found PDF: {pdf_url}")
            except Exception as e:
                logger.error(f"Error retrieving image or PDF for book {book_link}: {e}")
                continue

            # Download and upload the image to S3
            try:
                image_response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]

                # Check if the file is indeed an image
                if image_filename.endswith(('jpg', 'jpeg', 'png')):
                    s3.put_object(Bucket=S3_BUCKET_NAME, Key=f'images/{image_filename}', Body=image_response.content)
                    logger.info(f"Uploaded image {image_filename} to S3.")
                else:
                    logger.warning(f"Skipping non-image file: {image_filename}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download or upload image: {e}")
                continue

            # Download and upload the PDF to S3
            try:
                pdf_response = requests.get(pdf_url)
                pdf_filename = pdf_url.split('/')[-1]

                # Check if the file is indeed a PDF
                if pdf_filename.endswith('.pdf'):
                    s3.put_object(Bucket=S3_BUCKET_NAME, Key=f'pdfs/{pdf_filename}', Body=pdf_response.content)
                    logger.info(f"Uploaded PDF {pdf_filename} to S3.")
                else:
                    logger.warning(f"Skipping non-PDF file: {pdf_filename}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download or upload PDF: {e}")
                continue

            # Navigate back to the book list page
            driver.back()
            logger.info("Navigated back to the main page.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        driver.quit()
        logger.info("WebDriver session closed.")

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Update to pendulum if targeting Airflow 3.0 compatibility
}

dag = DAG(
    'scrape_images_pdfs_to_s3',
    default_args=default_args,
    description='Scrape images and PDFs from CFA and store them in S3',
    schedule_interval=None,
)

scrape_task = PythonOperator(
    task_id='scrape_and_store_files_in_s3',
    python_callable=scrape_and_upload_to_s3,
    dag=dag,
)