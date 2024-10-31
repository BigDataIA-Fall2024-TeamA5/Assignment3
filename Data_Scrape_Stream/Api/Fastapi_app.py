# Import necessary modules from FastAPI, Pydantic, and authentication libraries
import sys
import os
import boto3
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
import snowflake.connector  # Snowflake connector for database
from dotenv import load_dotenv  # For loading environment variables
import logging
import urllib.parse
import PyPDF2
import re
import requests




# Load environment variables from .env
load_dotenv()

# Add the directory of this file (Fastapi_app.py) to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print("Python Path:", sys.path)

# Retrieve AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Function to connect to Snowflake
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"), 
        password=os.getenv("SNOWFLAKE_PASSWORD"), 
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=os.getenv("SNOWFLAKE_HOST"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"), 
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn

# Set constants for JWT token generation
SECRET_KEY = "your-secret-key"  # Replace with a strong, random key in production
ALGORITHM = "HS256"  # Algorithm used for token encoding
ACCESS_TOKEN_EXPIRE_MINUTES = 15 # Token expiration time (in minutes)

# Create an instance of FastAPI
app = FastAPI()

# Initialize password hashing utility (using bcrypt for security)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Create an OAuth2 scheme to secure the login endpoint
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

# Function to hash the password
def get_password_hash(password):
    return pwd_context.hash(password)

# Function to verify if a plain password matches the hashed password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Function to create a JWT token with expiration
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# In-memory user database (Replace with a real database later)
users_db = {}

# Pydantic model for the user data
class User(BaseModel):
    username: str
    email: str
    password: str

# Route to handle user registration
@app.post("/auth/register")
async def register(user: User):
    # Check if username is already registered
    if user.username in users_db:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    # Hash the password and store the user in the in-memory database
    hashed_password = get_password_hash(user.password)
    users_db[user.username] = {
        "username": user.username,
        "email": user.email,
        "password": hashed_password
    }
    return {"message": "User registered successfully"}

# Route to handle user login and return a JWT token
@app.post("/auth/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    # Retrieve the user from the in-memory database
    user = users_db.get(form_data.username)
    if not user or not verify_password(form_data.password, user['password']):
        raise HTTPException(status_code=400, detail="Invalid credentials")
    
    # Create a JWT token for the user
    access_token = create_access_token(
        data={"sub": form_data.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

# Example protected route to fetch user information using the JWT token
@app.get("/auth/me")
async def read_users_me(token: str = Depends(oauth2_scheme)):
    try:
        # Decode and verify the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    # Return the user's information (for demonstration purposes)
    return {"username": username}

# Define a route for the root path
@app.get("/")
async def root():
    return {"message": "Welcome to the API!"}



@app.get("/test-snowflake/")
async def test_snowflake():
    try:
        # Connect to Snowflake and fetch some test data
        conn = snowflake.connector.connect(
            user='YOUR_USERNAME',
            password='YOUR_PASSWORD',
            account='YOUR_ACCOUNT',
            warehouse='YOUR_WAREHOUSE',
            database='YOUR_DATABASE',
            schema='YOUR_SCHEMA'
        )
        cursor = conn.cursor()
        query = "SELECT title, summary, image_link, pdf_link FROM documents"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if rows:
            return {"status": "success", "data": rows}
        else:
            return {"status": "success", "data": []}
        
    except Exception as e:
        logging.error(f"Error in test-snowflake endpoint: {e}")
        return {"status": "Failed", "error": str(e)}
    
    # Create an S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

@app.get("/")
def root():
    return {"message": "S3 connection test"}

@app.get("/list-s3-objects")
def list_s3_objects():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            return {"files": files}
        else:
            return {"message": "No files found in the bucket."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list objects: {str(e)}")

@app.get("/check-s3-connection")
def check_s3_connection():
    try:
        # Attempt to access the bucket to confirm the connection
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        return {"status": "Success", "message": "Connected to S3 bucket successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

from transformers import pipeline  # Load a BART model for summarization

summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# Configure logging
logging.basicConfig(level=logging.INFO)

# Import any additional libraries if needed
import urllib.parse
import logging
from fastapi import HTTPException

# Assuming summarizer and s3_client are already defined

# Helper function to clean text (optional but recommended for better summaries)
def clean_text(text):
    text = re.sub(r"\s{2,}", " ", text)  # Replace multiple spaces with a single space
    text = re.sub(r"[^\w\s.,!?]", "", text)  # Remove special characters except basic punctuation
    return text.strip()

from transformers import pipeline
import re
import logging
import PyPDF2

# Initialize summarization pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

from transformers import pipeline
import re
import logging
import PyPDF2

# Initialize summarization pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

from transformers import pipeline
import re
import logging
import PyPDF2

# Initialize summarization pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

from transformers import pipeline
import urllib.parse
import logging
import re
import PyPDF2
from fastapi import FastAPI, HTTPException

# Initialize T5 summarizer
summarizer = pipeline("summarization", model="t5-small")

app = FastAPI()

# Define your S3 bucket and client here (assuming s3_client and S3_BUCKET_NAME are set)

from transformers import pipeline
import re
import logging
import PyPDF2

from transformers import pipeline
import re
import logging
import PyPDF2

# Initialize summarization pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# Endpoint to download and summarize PDF with longer summary requirement
@app.get("/download-and-summarize/{file_key}")
def download_and_summarize(file_key: str):
    logging.info(f"Received file_key: {file_key}")
    encoded_file_key = urllib.parse.quote(file_key, safe="/")
    logging.info(f"Encoded file_key for S3 request: {encoded_file_key}")
    local_path = f"/tmp/{file_key.split('/')[-1]}"

    try:
        # Download file from S3
        s3_client.download_file(S3_BUCKET_NAME, encoded_file_key, local_path)
        logging.info(f"File downloaded successfully: {local_path}")
        
        # Extract text from the PDF
        with open(local_path, "rb") as f:
            pdf_reader = PyPDF2.PdfReader(f)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
        
        # Clean and split text into meaningful sections
        text = re.sub(r'\s+', ' ', text)
        sections = text.split('\n\n')
        
        # Summarize each section individually with a larger max_length for detailed summaries
        summary_text = ""
        for section in sections:
            if len(section) > 100:  # Exclude very short sections
                try:
                    # Using a higher max_length to ensure longer, more detailed summaries
                    summary = summarizer(section, max_length=300, min_length=100, do_sample=False)
                    summary_text += summary[0]["summary_text"] + " "
                except Exception as e:
                    logging.warning(f"Skipping section due to summarization error: {e}")
        
        logging.info("Summarization completed successfully.")
        return {"summary": summary_text.strip()}
        
    except Exception as e:
        logging.error(f"Failed to download or summarize file '{encoded_file_key}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to download or summarize: {str(e)}")

    
    # Define FastAPI endpoint to list PDF files in S3
@app.get("/list-pdfs")
async def list_pdfs_in_s3():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="pdfs/")
        if 'Contents' in response:
            pdf_files = [obj['Key'] for obj in response['Contents']]
            return {"pdf_files": pdf_files}
        else:
            return {"message": "No files found in the 'pdfs' directory."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")
