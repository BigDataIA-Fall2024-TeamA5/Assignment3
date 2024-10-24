import streamlit as st
import requests
from app_webpage import display_user_info  # Import the function to display user info

# Set FastAPI backend URL
backend_url = "http://127.0.0.1:8001"

# State management for navigation
if "page" not in st.session_state:
    st.session_state.page = "register"

# Function to go to the login page
def go_to_login():
    st.session_state.page = "login"

# Function to go to the register page
def go_to_register():
    st.session_state.page = "register"

# Function to go to the user info page
def go_to_user_info():
    st.session_state.page = "user_info"

# Registration Page
if st.session_state.page == "register":
    st.title("Register")
    username = st.text_input("Enter your username")
    email = st.text_input("Enter your email")
    password = st.text_input("Enter your password", type="password")

    if st.button("Register"):
        if username and email and password:
            response = requests.post(
                f"{backend_url}/auth/register",
                json={"username": username, "email": email, "password": password}
            )
            if response.status_code == 200:
                st.success("Registration successful! Please log in.")
                go_to_login()  # Automatically move to the login page
            else:
                st.error(f"Error: {response.json()['detail']}")
        else:
            st.error("Please fill in all fields.")
    
    st.button("Go to Login", on_click=go_to_login)

# Login Page
elif st.session_state.page == "login":
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username and password:
            response = requests.post(
                f"{backend_url}/auth/login",
                data={"username": username, "password": password},
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            if response.status_code == 200:
                access_token = response.json().get("access_token")
                st.session_state["access_token"] = access_token  # Save token
                st.success("Login successful!")
                go_to_user_info()  # Navigate to User Info page
            else:
                st.error("Invalid credentials.")
        else:
            st.error("Please enter both username and password.")

    st.button("Go to Register", on_click=go_to_register)

# User Info Page (loaded from app_webpage.py)
elif st.session_state.page == "user_info":
    display_user_info(backend_url)  # Call the function from app_webpage.py
