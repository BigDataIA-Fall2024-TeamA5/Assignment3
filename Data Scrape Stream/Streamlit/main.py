import streamlit as st
import requests
import boto3
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# S3 Credentials from .env
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("S3_REGION")

# FastAPI endpoint URLs for user login and PDF list retrieval
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://127.0.0.1:8000")
REGISTER_URL = f"{FASTAPI_URL}/auth/register"
LOGIN_URL = f"{FASTAPI_URL}/auth/login"

# Set up Streamlit page configuration
st.set_page_config(page_title="PDF Text Extraction Application", layout="centered")

# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'access_token' not in st.session_state:
    st.session_state['access_token'] = None
if 'pdf_files' not in st.session_state:
    st.session_state['pdf_files'] = []
if 'selected_pdf' not in st.session_state:
    st.session_state['selected_pdf'] = None
if 'view_mode' not in st.session_state:
    st.session_state['view_mode'] = 'list'  # default view is list

# S3 client setup
def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION
    )

# Function to fetch PDFs from S3
def list_pdfs_from_s3():
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            pdf_files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.pdf')]
            return pdf_files
        else:
            return []
    except Exception as e:
        st.error(f"Failed to fetch PDFs from S3: {e}")
        return []

# Main Application
def main_app():
    st.title("PDF Text Extraction Application")

    # Fetch PDF files from S3 bucket
    if not st.session_state['pdf_files']:
        st.session_state['pdf_files'] = list_pdfs_from_s3()

    # Let the user choose between grid view and list view
    view_mode = st.radio("Select view mode", ["List View", "Grid View"], index=0 if st.session_state['view_mode'] == 'list' else 1)

    # Update session state based on view mode
    if view_mode == "List View":
        st.session_state['view_mode'] = 'list'
    else:
        st.session_state['view_mode'] = 'grid'

    # Display PDFs based on selected view mode
    if st.session_state['view_mode'] == 'list':
        display_pdfs_list_view()
    else:
        display_pdfs_grid_view()

# Function to display PDFs in list view
def display_pdfs_list_view():
    st.subheader("PDF Files (List View)")
    for pdf in st.session_state['pdf_files']:
        if st.button(f"Open {pdf}"):
            st.session_state['selected_pdf'] = pdf
            show_pdf_details(pdf)

# Function to display PDFs in grid view
def display_pdfs_grid_view():
    st.subheader("PDF Files (Grid View)")
    cols = st.columns(3)  # Adjust the number of columns as needed
    for i, pdf in enumerate(st.session_state['pdf_files']):
        with cols[i % 3]:
            if st.button(f"Open {pdf}"):
                st.session_state['selected_pdf'] = pdf
                show_pdf_details(pdf)

# Function to display PDF details
def show_pdf_details(pdf_name):
    st.write(f"### Details of {pdf_name}")
    st.write(f"*File Name*: {pdf_name}")
    
    # Assuming you store some metadata in the S3 object or a database
    # You can fetch additional metadata here if needed
    st.write("Additional information about the PDF...")

    # Link to the PDF file on S3 for downloading or viewing
    s3_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{pdf_name}"
    st.markdown(f"[Open PDF]({s3_url})", unsafe_allow_html=True)

# Login Page
def login_page():
    option = st.selectbox("Select Login or Signup", ("Login", "Signup"))

    if option == "Login":
        st.subheader("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            login(username, password)

    elif option == "Signup":
        st.subheader("Signup")
        username = st.text_input("Username")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        if st.button("Signup"):
            signup(username, email, password)

# Fetch the list of PDFs from FastAPI
def login(username, password):
    response = requests.post(LOGIN_URL, json={
        "username": username,
        "password": password
    })
    if response.status_code == 200:
        token_data = response.json()
        st.session_state['access_token'] = token_data['access_token']
        st.session_state['logged_in'] = True
        st.success("Logged in successfully!")
    else:
        st.error("Invalid username or password. Please try again.")

# Main Interface depending on login state
if st.session_state['logged_in']:
    main_app()
else:
    login_page()
