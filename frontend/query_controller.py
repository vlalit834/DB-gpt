from typing import Dict, Any
from backend.server import execute, get_schema
from frontend.deepseek_api import generate_sql

async def query_controller(database: str, user_input: str) -> Dict[str, Any]:
    """Complete query processing workflow"""
    if not user_input or len(user_input) > 500:
        return {'error': 'Invalid input or input too long'}

    schema_result = await get_schema(database)
    if 'error' in schema_result:
        return schema_result

    print(f"User Input: {user_input}")
    sql = generate_sql(user_input, schema_result['schema'])
    print(f"Generated SQL: {sql}")

    result = await execute(database, sql)

    result['generated_sql'] = sql
    return result
