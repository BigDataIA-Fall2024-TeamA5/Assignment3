import os
import snowflake.connector
import requests
from transformers import AutoTokenizer, AutoModelForCausalLM
from dotenv import load_dotenv
from transformers import AutoModelForCausalLM, AutoTokenizer
import yaml
# Load environment variables from .env file
load_dotenv()

# Fetch credentials from environment variables
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)


# Load the YAML configuration manually
with open('/Users/aniketpatole/Documents/GitHub/openai/Assignment3/Data Scrape Stream/Api/Summary/model_config.yaml', 'r') as f:
    model_config = yaml.safe_load(f)

# Replace model_name with the direct path if needed
model_name = "nvidia/Llama-3.1-Nemotron-70B-Instruct"

# Load the model using the YAML config
tokenizer = AutoTokenizer.from_pretrained(model_name, config=model_config)
model = AutoModelForCausalLM.from_pretrained(model_name, config=model_config)
# Fetch PDF link and download
cursor = conn.cursor()
cursor.execute("SELECT ID, PDF_LINK FROM PUBLICATIONS WHERE NVIDIA_SUMMARY IS NULL")
results = cursor.fetchall()

for row in results:
    pdf_id, pdf_link = row
    
    # Download the PDF
    response = requests.get(pdf_link)
    pdf_path = f"/path/to/save/{pdf_id}.pdf"
    with open(pdf_path, 'wb') as pdf_file:
        pdf_file.write(response.content)
    
    # Add PDF text extraction logic here, and generate a summary
    
    # Generate summary using the model
    text = "Extracted PDF text goes here"
    inputs = tokenizer(text, return_tensors="pt")
    summary_ids = model.generate(inputs["input_ids"], max_length=150)
    summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)

    # Save the summary back to Snowflake
    cursor.execute("""
        UPDATE PUBLICATIONS SET NVIDIA_SUMMARY = %s WHERE ID = %s
    """, (summary, pdf_id))
    
    # Remove the downloaded PDF file
    os.remove(pdf_path)

# Close the connection
conn.commit()
cursor.close()
conn.close()