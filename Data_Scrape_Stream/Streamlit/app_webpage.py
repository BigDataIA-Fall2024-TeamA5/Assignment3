import streamlit as st
import requests

# Function to display the user info page
def display_user_info(backend_url):
    st.title("User Info")
    
    if "access_token" in st.session_state:
        token = st.session_state["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{backend_url}/auth/me", headers=headers)
        
        if response.status_code == 200:
            user_info = response.json()
            st.write("User Information:")
            st.json(user_info)
        else:
            st.error("Failed to fetch user info.")
    else:
        st.warning("Please log in to view your user info.")
        st.session_state.page = "login"  # Navigate back to login if no token is found
