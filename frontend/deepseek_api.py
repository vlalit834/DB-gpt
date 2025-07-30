import os
import requests
from typing import Dict, Any
from dotenv import load_dotenv
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.core.credentials import AzureKeyCredential

load_dotenv()
class DeepseekAPI:
    def __init__(self):
        self.config = {
            'api_key': 'your_api_key_here',
            'endpoint': 'https://api.deepseek.com/v1'
        }

    def query(self, prompt: str) -> str:
        return f"AI Response: Received your query '{prompt}'"

def generate_sql(natural_language: str, schema: Dict[str, Any]) -> str:
    """Generate SQL"""
    if not natural_language or len(natural_language) > 500:
        return "SELECT 1"  

    prompt = f"""
    ## Task Description
    You are a professional SQL generator. Your task is to convert natural language queries into accurate MySQL SQL statements.
    The current database contains the following schema:
    {schema}

    ## Generation Rules
    1. You must use the exact table names and column names provided.
    2. Return only valid SQL statements without any explanations or comments.
    3. When WHERE conditions are used, ensure correct data type comparison.
    4. If the query might return many results, automatically add LIMIT 250.
    5. Use the standard date format: 'YYYY-MM-DD'.
    6. Use single quotes for string comparison and treat case sensitively.

    ## Query Examples
    Input: Find students whose last name is Zhang
    Output: SELECT * FROM student WHERE name LIKE '张%'

    Input: Find the top 10 students enrolled in 2023
    Output: SELECT * FROM student WHERE enrollment_date BETWEEN '2023-01-01' AND '2023-12-31' LIMIT 10

    Input: Count the number of students in each class
    Output: SELECT class_id, COUNT(*) AS student_count FROM student GROUP BY class_id
    
    ## Special Instructions
    Please avoid using nested subqueries. Prefer the following alternatives:
    1. Use JOIN instead of subqueries in WHERE clauses.
    2. Use GROUP BY and HAVING instead of correlated subqueries.
    3. Use window functions to replace complex subqueries.
    4. Use temporary table logic to rewrite queries.

    ## Examples
    Don't: SELECT * FROM table1 WHERE id IN (SELECT id FROM table2)
    Do: SELECT table1.* FROM table1 JOIN table2 ON table1.id = table2.id

    Don't: SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.cust_id = customers.id) 
    Do: SELECT c.name, COUNT(o.id) FROM customers c LEFT JOIN orders o ON c.id = o.cust_id GROUP BY c.id

    ## Query to Convert
    \"\"\"{natural_language}\"\"\"
    """

    try:
        endpoint = "https://models.github.ai/inference"
        model    = "deepseek/DeepSeek-V3-0324"
        token    = os.getenv("GITHUB_TOKEN")
        if not token:
            raise RuntimeError("GITHUB_TOKEN is not set in your environment")
        
        client   = ChatCompletionsClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(token),
        )
        
        completion = client.complete(
            messages=[
                SystemMessage(""),              
                UserMessage(prompt)
            ],
            model=model,
            temperature=0.1,
            top_p=0.1,
            max_tokens=1024
        )
        raw_sql = completion.choices[0].message.content.strip()

        if "```sql" in raw_sql:
            raw_sql = raw_sql.split("```sql")[1].split("```")[0].strip()

        return raw_sql

    except requests.exceptions.HTTPError as e:
        print(f"[HTTP Error] {e.response.status_code}: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"[Network Request Error] {str(e)}")
    except (KeyError, ValueError, IndexError) as e:
        print(f"[Response Parsing Error] {str(e)}")
        print(f"[Full API Response] {response.text if 'response' in locals() else 'No response data'}")
    except Exception as e:
        print(f"[Unknown Error] {str(e)}")

    return "SELECT * FROM student LIMIT 3"
