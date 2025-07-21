import os
import asyncio
from typing import Any, Dict
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlparse import parse
from mcp.server.fastmcp import FastMCP
from typing import List
import re
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DML
import logging
from datetime import datetime

load_dotenv()
mcp = FastMCP('sql')

########################################################################
######################## Connect to the database #####################
########################################################################
def create_db_engine(database: str = None):
    """Create a database engine with connection pool"""
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST', 'localhost')
    port = os.getenv('MYSQL_PORT', '3306')
    pool_size = int(os.getenv('POOL_SIZE', '5'))

    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    if database:
        db_url = f"{db_url}/{database}"

    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False  
    )

@mcp.tool
async def test_connection() -> Dict[str, Any]:
    """Test database connection"""
    try:
        engine = create_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return {'status': 'Connection successful'}
    except Exception as e:
        return {'error': str(e)}
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    test_result=asyncio.run(test_connection())
    print("Database Connection Test: ", test_result)
    mcp.run(transport='stdio')