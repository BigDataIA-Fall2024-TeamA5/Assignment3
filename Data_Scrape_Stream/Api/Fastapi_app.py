# Import necessary modules from FastAPI, Pydantic, and authentication libraries
import sys
import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
import snowflake.connector  # Snowflake connector for database
from dotenv import load_dotenv  # For loading environment variables

# Load environment variables from .env
load_dotenv()

# Add the directory of this file (Fastapi_app.py) to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print("Python Path:", sys.path)

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



@app.get("/test-snowflake")
async def test_snowflake_connection():
    try:
        # Attempt to establish a connection
        conn = get_snowflake_connection()
        # Optionally, run a simple query to verify connection
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")  # Sample query
        result = cursor.fetchone()
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return {"status": "Success", "Snowflake_version": result[0]}
    except Exception as e:
        return {"status": "Failed", "error": str(e)}