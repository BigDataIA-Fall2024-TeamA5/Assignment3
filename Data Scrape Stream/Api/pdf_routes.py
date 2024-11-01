from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from transformers import pipeline
import snowflake.connector
import boto3
import requests
from io import BytesIO
from PyPDF2 import PdfReader
from jwtauth import get_current_user
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "PUBLICATIONS"  # Directly specifying the table name

# AWS S3 configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Initialize summarization model (Llama or similar)
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# Router
pdf_router = APIRouter()

class SummaryRequest(BaseModel):
    pdf_id: int

def get_pdf_link(pdf_id: int) -> str:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    try:
        # Fetch the PDF key (path in S3) from the PUBLICATIONS table based on pdf_id
        query = "SELECT PDF_LINK FROM PUBLICATIONS WHERE ID = %s"
        cursor = conn.cursor()
        cursor.execute(query, (pdf_id,))
        result = cursor.fetchone()
        pdf_key = result[0] if result else None  # The S3 key should be stored in PDF_LINK

        if pdf_key:
            # Generate a pre-signed URL for the PDF file in S3
            signed_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': S3_BUCKET_NAME, 'Key': pdf_key},
                ExpiresIn=3600  # URL expires in 1 hour
            )
            return signed_url
        else:
            raise ValueError("PDF link not found in database.")
    finally:
        conn.close()

def extract_pdf_text(pdf_url: str) -> str:
    # Download the PDF from the signed URL
    response = requests.get(pdf_url)
    response.raise_for_status()

    # Read PDF content
    pdf_text = ""
    pdf_reader = PdfReader(BytesIO(response.content))
    for page in pdf_reader.pages:
        pdf_text += page.extract_text()

    return pdf_text

@pdf_router.post("/generate-summary")
async def generate_summary(request: SummaryRequest, current_user: str = Depends(get_current_user)):
    try:
        # Get the signed URL for the PDF from the database
        pdf_link = get_pdf_link(request.pdf_id)
        if not pdf_link:
            raise HTTPException(status_code=404, detail="PDF link not found in database.")

        # Extract text from the PDF using the signed URL
        pdf_text = extract_pdf_text(pdf_link)

        # Generate summary using the summarization model
        summary = summarizer(pdf_text, max_length=150, min_length=50, do_sample=False)
        return {"summary": summary[0]['summary_text']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate summary: {str(e)}")
