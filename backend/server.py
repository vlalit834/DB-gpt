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
######################## Connect to the database #######################
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

@mcp.tool()
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

########################################################################
######################## Get Schema ####################################
########################################################################

@mcp.tool()
async def get_schema(database: str, table_name: str = None) -> Dict[str, Any]:
    """Get simplified table schema (supports table name filtering)"""
    try:
        engine = create_db_engine(database)
        with engine.connect() as conn:
            tables = conn.execute(text("SHOW TABLES")).scalars().fetchall()
            if table_name:
                if table_name not in tables:
                    return {'error': f'Table {table_name} does not exist'}
                tables = [table_name]
            schema = {}
            for table in tables:
                columns = conn.execute(
                    text(f"SHOW COLUMNS FROM {table}")
                ).fetchall()
                schema[table] = [
                    {'field': col[0], 'type': col[1].split('(')[0]} 
                    for col in columns
                ]
            return {'schema': schema}
    except Exception as e:
        return {'error': f"Failed to get table schema: {str(e)}"}
    finally:
        if 'engine' in locals():
            engine.dispose()

########################################################################
######################## Execute the query #############################
########################################################################

SENSITIVE_FIELDS = {
    'password', 'pwd', 'secret', 'salary'
}
def has_sensitive_fields(query: str) -> bool:
    """Check if the query involves sensitive fields"""
    query_lower = query.lower()
    if any(field in query_lower for field in SENSITIVE_FIELDS):
        return True
    return False

def log_query(database: str, query: str, status: str = "SUCCESS"):
    """Log query execution"""
    logging.info(
        f"[{status}] DB={database} | "
        f"SQL={query[:200]}{'...' if len(query) > 200 else ''}"
    )
    logging.getLogger().handlers[0].flush()

def detect_sql_injection(query: str) -> bool:
    """SQL injection detection"""
    normalized = ' '.join(query.lower().strip().split())
    if ';' in normalized[:-1]: 
        return True

    dangerous_operations = [
        'drop table', 'truncate table', 'delete from',
        'insert into', 'update table', 'alter table',
        'create table', 'rename table', 'shutdown'
    ]
    if any(op in normalized for op in dangerous_operations):
        return True

    if re.search(r'(/\*|--\s)', normalized):
        return True

    clean_query = re.sub(r'\\[\'\"]', '', query)
    if (clean_query.count("'") % 2 != 0) or (clean_query.count('"') % 2 != 0):
        return True
    
    return False

def is_readonly_query(parsed: Statement) -> bool:
    """Whitelist filter to check if it's a read-only SELECT statement"""
    for token in parsed.tokens:
        if token.ttype is DML:
            return token.value.upper() == 'SELECT'
    return True

@mcp.tool()
async def execute(database: str, query: str) -> Dict[str, Any]:
    """Execute SQL query"""
    try:
        start_time = datetime.now()

        if has_sensitive_fields(query):
            log_query(database, query, "BLOCKED_SENSITIVE_FIELD")
            return {'error': 'Query contains sensitive fields'}

        if detect_sql_injection(query):
            log_query(database, query, "SQL_INJECTION")
            return {'error': 'Potential SQL injection risk detected'}

        parsed = sqlparse.parse(query)[0]
        if not is_readonly_query(parsed):
            log_query(database, query, "SELECT_ONLY")
            return {'error': 'Only SELECT queries are allowed'}

        schema_result = await get_schema(database)
        if 'error' in schema_result:
            return schema_result

        valid_tables = schema_result['schema'].keys()

        stmt = parse(query)[0]

        from sqlparse.sql import Identifier
        query_tables = []
        for token in stmt.tokens:
            if isinstance(token, Identifier):
                table_name = token.get_real_name()
                if table_name in valid_tables:
                    query_tables.append(table_name)

        if not query_tables:
            return {'error': 'No valid table name used in SQL'}

        engine = create_db_engine(database)
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(query))
                if result.returns_rows:
                    columns = result.keys()
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
                    exec_time = (datetime.now() - start_time).total_seconds()
                    log_query(database, f"{query} [ExecutionTime={exec_time:.3f}s]")
                    return {'results': rows}
                else:
                    return {
                        'results': {
                            'rowcount': result.rowcount,
                            'message': 'Non-query operation executed successfully'
                        }
                    }
    except Exception as e:
        log_query(database, query, f"ERROR_{str(e)}")
        return {'error': f"SQL execution error: {str(e)}"}
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    test_result=asyncio.run(test_connection())
    print("Database Connection Test: ", test_result)
    mcp.run(transport='stdio')