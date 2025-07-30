## Project Overview

This project is a MySQL database query system based on MCP, providing a graphical interface via Streamlit. It enables interaction between natural language and the database system through an MCP interface to query relevant information, while implementing various enhanced features and security controls.

## Table of Contents

- [Features](#features)
- [Installation and Execution](#installation-and-execution)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [User Guide](#user-guide)
- [Technical Architecture](#technical-architecture)

## Features

### Core Features

- **MCP Service Operation**: Connects to a MySQL instance via an MCP server
- **Natural Language to SQL**: Converts natural language into precise SQL queries through the MCP interface
- **Database Schema Visualization**: Visually displays table schemas and query result information

### Advanced Features

- **SQL Security Protection**: Built-in SQL injection detection, sensitive field filtering, and read-only SQL whitelist filtering
- **Simplified Schema Output**: Supports filtering returned schema by table name
- **Query Result Pagination**: Automatically paginates long results in the GUI
- **Query Performance Optimization**: Automatically optimizes generated SQL statements
- **Query Logging**: MCP server logs each executed SQL statement with timestamps
- **Data Export**: One-click export of query results to CSV format

## Installation and Execution

### Prerequisites

- Python 3.10
- MySQL 8.0+ database
- API key

### Installation Steps

#### 1. Clone the Repository

```bash
git clone https://github.com/vlalit834/DB-gpt.git
cd DB-gpt
```

#### 2. Set Up Virtual Environment

```bash
uv venv
.\.venv\Scripts\activate
```

#### 3. Install Dependencies

```bash
uv pip install -r requirements.txt
```

#### 4. Configure Environment Variables

Create a `.env` file:

```
# Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
POOL_SIZE=5

# Deepseek API Configuration
GITHUB_TOKEN=
```

#### 5. Run the Application

```bash
#Start server
uv run .\backend\server.py

#Run frontend
streamlit run frontend/app.py
```

## User Guide

1. **Connect to the Database**

   - Enter the database name in the sidebar.
   - Test the connection to ensure correct configuration.

2. **Execute Queries**

   - Type your query in natural language in the input box. Examples:
     - "Show students from the History department"
     - "List all courses with prerequisites"

3. **Browse Results**

   - View the generated SQL statement.
   - Use pagination controls to navigate:
     - Select page size in the sidebar.
     - Click Next/Previous or enter a page number to jump.

4. **View Table Schema**

   - Check "Show Table Schema" to see detailed field information.
   - Enter a table name to retrieve its schema.

5. **Export Results**
   - Click "Export All Results to CSV" to save the results.

## Technical Architecture

### Core Components

1. **Frontend Interface**: Responsive GUI built with Streamlit
2. **LLM API Interaction**: Deepseek for natural language processing
3. **MCP Service**: Communicates with the MySQL database via MCP
