"""
Business Intelligence Chatbot
Transformation from: Personal Chatbot → Data Query Assistant
Target: Small business owners, non-technical managers  
Value Prop: "Get business insights in plain English, not spreadsheets"
"""

import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import os

# Mock OpenAI client for demo (replace with real OpenAI when API key available)
class MockOpenAI:
    def __init__(self):
        pass
    
    def generate_sql_query(self, natural_query: str, table_schema: Dict) -> str:
        """Convert natural language to SQL query"""
        query_lower = natural_query.lower()
        
        # Simple pattern matching for demo
        if 'sales' in query_lower and 'last month' in query_lower:
            return "SELECT SUM(amount) as total_sales FROM sales WHERE date >= date('now', '-1 month')"
        elif 'revenue' in query_lower and 'by product' in query_lower:
            return "SELECT product_name, SUM(amount) as revenue FROM sales GROUP BY product_name ORDER BY revenue DESC"
        elif 'customers' in query_lower and 'this year' in query_lower:
            return "SELECT COUNT(DISTINCT customer_id) as customer_count FROM sales WHERE date >= date('now', 'start of year')"
        elif 'top' in query_lower and 'customers' in query_lower:
            return "SELECT customer_name, SUM(amount) as total_spent FROM sales GROUP BY customer_name ORDER BY total_spent DESC LIMIT 10"
        elif 'monthly' in query_lower and 'trend' in query_lower:
            return "SELECT strftime('%Y-%m', date) as month, SUM(amount) as monthly_sales FROM sales GROUP BY month ORDER BY month"
        else:
            return "SELECT * FROM sales LIMIT 10"
    
    def generate_insights(self, query_result: pd.DataFrame, original_query: str) -> str:
        """Generate natural language insights from query results"""
        if query_result.empty:
            return "No data found for your query."
        
        insights = []
        
        # Check for trends in numerical data
        numeric_cols = query_result.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            for col in numeric_cols:
                total = query_result[col].sum()
                avg = query_result[col].mean()
                insights.append(f"Total {col.replace('_', ' ')}: ${total:,.2f}")
                insights.append(f"Average {col.replace('_', ' ')}: ${avg:,.2f}")
        
        # Check for top performers
        if len(query_result) > 1:
            first_row = query_result.iloc[0]
            if 'name' in str(first_row.index).lower() or 'customer' in str(first_row.index).lower():
                name_col = [col for col in query_result.columns if 'name' in col.lower() or 'customer' in col.lower()]
                if name_col:
                    top_performer = first_row[name_col[0]]
                    insights.append(f"Top performer: {top_performer}")
        
        return "\\n".join(insights) if insights else "Data retrieved successfully."

@dataclass
class BusinessDataConnection:
    name: str
    type: str  # 'sqlite', 'csv', 'api'
    connection_string: str
    tables: List[str]
    description: str

class BusinessDataBot:
    def __init__(self, db_path="business_data.db"):
        self.db_path = db_path
        self.llm_client = MockOpenAI()  # Replace with OpenAI() when API key available
        self.init_demo_database()
        self.conversation_history = []
    
    def init_demo_database(self):
        """Initialize demo business database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create sales table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                customer_id INTEGER,
                customer_name TEXT,
                product_name TEXT,
                category TEXT,
                amount REAL,
                quantity INTEGER,
                sales_rep TEXT,
                region TEXT
            )
        ''')
        
        # Create customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                registration_date TEXT,
                customer_type TEXT,
                credit_limit REAL
            )
        ''')
        
        # Create products table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                price REAL,
                cost REAL,
                stock_quantity INTEGER,
                supplier TEXT
            )
        ''')
        
        # Generate demo data if tables are empty
        cursor.execute("SELECT COUNT(*) FROM sales")
        if cursor.fetchone()[0] == 0:
            self.generate_demo_business_data(cursor)
        
        conn.commit()
        conn.close()
    
    def generate_demo_business_data(self, cursor):
        """Generate realistic demo business data"""
        np.random.seed(42)
        
        # Products
        products = [
            ('Laptop Pro', 'Electronics', 1299.99, 800.00, 45, 'TechSupply Inc'),
            ('Wireless Mouse', 'Electronics', 29.99, 15.00, 120, 'TechSupply Inc'),
            ('Office Chair', 'Furniture', 249.99, 150.00, 30, 'FurnCorp'),
            ('Desk Lamp', 'Furniture', 89.99, 45.00, 75, 'LightCo'),
            ('Coffee Maker', 'Appliances', 129.99, 80.00, 25, 'ApplianceWorld'),
            ('Water Bottle', 'Accessories', 19.99, 8.00, 200, 'LifeStyle Ltd'),
            ('Notebook Set', 'Office Supplies', 24.99, 12.00, 150, 'PaperCorp'),
            ('Smartphone', 'Electronics', 899.99, 600.00, 60, 'TechSupply Inc'),
            ('Standing Desk', 'Furniture', 599.99, 350.00, 15, 'FurnCorp'),
            ('Headphones', 'Electronics', 199.99, 120.00, 80, 'AudioTech')
        ]
        
        for product in products:
            cursor.execute('''
                INSERT INTO products (name, category, price, cost, stock_quantity, supplier)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', product)
        
        # Customers
        customer_names = [
            'Acme Corporation', 'Global Solutions LLC', 'TechStart Inc', 'Creative Agency',
            'Retail Plus', 'Manufacturing Corp', 'Service Pro', 'Innovation Labs',
            'Digital Marketing Co', 'Consulting Group', 'Local Restaurant',
            'Healthcare Partners', 'Education Foundation', 'Non-Profit Org',
            'Construction Company', 'Real Estate Group', 'Financial Services',
            'Transportation LLC', 'Energy Solutions', 'Food Distribution'
        ]
        
        customer_types = ['Enterprise', 'Small Business', 'Startup', 'Non-Profit']
        
        for i, name in enumerate(customer_names):
            cursor.execute('''
                INSERT INTO customers (name, email, phone, registration_date, customer_type, credit_limit)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                name,
                f"contact@{name.lower().replace(' ', '').replace(',', '')}.com",
                f"555-{1000+i:04d}",
                (datetime.now() - timedelta(days=np.random.randint(30, 730))).strftime('%Y-%m-%d'),
                np.random.choice(customer_types),
                np.random.randint(5000, 50000)
            ))
        
        # Generate sales data for the last 12 months
        sales_reps = ['Alice Johnson', 'Bob Smith', 'Carol Williams', 'David Brown']
        regions = ['North', 'South', 'East', 'West']
        
        for _ in range(1000):  # 1000 sales records
            # Random date in the last 12 months
            random_days_ago = np.random.randint(0, 365)
            sale_date = (datetime.now() - timedelta(days=random_days_ago)).strftime('%Y-%m-%d')
            
            customer_id = np.random.randint(1, 21)  # 20 customers
            product_id = np.random.randint(1, 11)   # 10 products
            quantity = np.random.randint(1, 10)
            
            # Get product info
            cursor.execute("SELECT name, category, price FROM products WHERE id = ?", (product_id,))
            product_info = cursor.fetchone()
            
            # Get customer info
            cursor.execute("SELECT name FROM customers WHERE id = ?", (customer_id,))
            customer_name = cursor.fetchone()[0]
            
            amount = product_info[2] * quantity * np.random.uniform(0.8, 1.0)  # Some discount variation
            
            cursor.execute('''
                INSERT INTO sales (date, customer_id, customer_name, product_name, category, 
                                 amount, quantity, sales_rep, region)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                sale_date, customer_id, customer_name, product_info[0], product_info[1],
                round(amount, 2), quantity, np.random.choice(sales_reps), np.random.choice(regions)
            ))
    
    def get_table_schema(self) -> Dict[str, List[str]]:
        """Get database schema for query generation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        schema = {}
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            schema[table_name] = columns
        
        conn.close()
        return schema
    
    def interpret_business_query(self, natural_language_query: str) -> str:
        """Convert natural language to SQL query"""
        schema = self.get_table_schema()
        sql_query = self.llm_client.generate_sql_query(natural_language_query, schema)
        return sql_query
    
    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results"""
        try:
            conn = sqlite3.connect(self.db_path)
            result_df = pd.read_sql_query(sql_query, conn)
            conn.close()
            return result_df
        except Exception as e:
            st.error(f"Query execution error: {str(e)}")
            return pd.DataFrame()
    
    def generate_business_insights(self, query_result: pd.DataFrame, original_query: str) -> str:
        """Generate natural language insights from query results"""
        return self.llm_client.generate_insights(query_result, original_query)
    
    def create_visualization(self, df: pd.DataFrame, query: str) -> Optional[go.Figure]:
        """Auto-generate appropriate visualization for the data"""
        if df.empty or len(df.columns) < 2:
            return None
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(exclude=[np.number]).columns
        
        # Time series detection
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'month' in col.lower() or 'time' in col.lower()]
        
        if len(date_cols) > 0 and len(numeric_cols) > 0:
            # Time series plot
            fig = px.line(df, x=date_cols[0], y=numeric_cols[0], 
                         title=f"{numeric_cols[0].replace('_', ' ').title()} Over Time")
            return fig
        
        elif len(categorical_cols) > 0 and len(numeric_cols) > 0:
            # Bar chart
            if len(df) <= 20:  # Limit for readability
                fig = px.bar(df, x=categorical_cols[0], y=numeric_cols[0],
                           title=f"{numeric_cols[0].replace('_', ' ').title()} by {categorical_cols[0].replace('_', ' ').title()}")
                fig.update_xaxis(tickangle=45)
                return fig
        
        return None
    
    def export_results(self, df: pd.DataFrame, format_type: str = 'csv') -> bytes:
        """Export query results to various formats"""
        if format_type == 'csv':
            return df.to_csv(index=False).encode('utf-8')
        elif format_type == 'excel':
            # For demo purposes, return CSV (would need openpyxl for Excel)
            return df.to_csv(index=False).encode('utf-8')
        else:
            return df.to_json(orient='records').encode('utf-8')

class BusinessIntelligenceChatbotApp:
    def __init__(self):
        self.bot = BusinessDataBot()
        self.setup_page_config()
    
    def setup_page_config(self):
        st.set_page_config(
            page_title="Business Intelligence Chatbot",
            page_icon="💬",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def display_sample_queries(self):
        """Show example queries users can try"""
        st.sidebar.markdown("### 💡 Try These Queries:")
        
        sample_queries = [
            "Show me sales for last month",
            "What's our revenue by product?",
            "How many customers do we have this year?", 
            "Who are our top 10 customers?",
            "Show monthly sales trends",
            "Which products are selling best?",
            "What's our average order value?",
            "Show sales by region"
        ]
        
        for query in sample_queries:
            if st.sidebar.button(query, key=f"sample_{hash(query)}"):
                return query
        
        return None
    
    def display_data_overview(self):
        """Display overview of available data"""
        st.sidebar.markdown("### 📊 Available Data")
        
        conn = sqlite3.connect(self.bot.db_path)
        
        # Sales summary
        sales_count = pd.read_sql_query("SELECT COUNT(*) as count FROM sales", conn).iloc[0]['count']
        customer_count = pd.read_sql_query("SELECT COUNT(*) as count FROM customers", conn).iloc[0]['count']
        product_count = pd.read_sql_query("SELECT COUNT(*) as count FROM products", conn).iloc[0]['count']
        
        st.sidebar.metric("Sales Records", f"{sales_count:,}")
        st.sidebar.metric("Customers", f"{customer_count:,}")
        st.sidebar.metric("Products", f"{product_count:,}")
        
        conn.close()
    
    def main_chat_interface(self):
        """Main chat interface"""
        st.title("💬 Business Intelligence Chatbot")
        st.markdown("*Ask questions about your business data in plain English*")
        
        # Initialize chat history in session state
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []
        
        # Sample query selection
        sample_query = self.display_sample_queries()
        
        # Data overview
        self.display_data_overview()
        
        # Query input
        st.markdown("---")
        query_input = st.text_input(
            "Ask a question about your business data:", 
            value=sample_query if sample_query else "",
            placeholder="e.g., 'Show me our top-selling products this quarter'"
        )
        
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            ask_button = st.button("🔍 Ask", type="primary")
        
        with col2:
            clear_button = st.button("🗑️ Clear History")
        
        if clear_button:
            st.session_state.chat_history = []
            st.experimental_rerun()
        
        # Process query
        if ask_button and query_input.strip():
            self.process_business_query(query_input.strip())
        
        # Display chat history
        if st.session_state.chat_history:
            st.markdown("---")
            st.subheader("💬 Conversation History")
            
            for i, chat in enumerate(reversed(st.session_state.chat_history[-5:])):  # Show last 5
                with st.expander(f"Q: {chat['query'][:50]}...", expanded=(i == 0)):
                    st.markdown(f"**Question:** {chat['query']}")
                    
                    if not chat['results'].empty:
                        st.markdown(f"**Answer:** {chat['insights']}")
                        
                        # Show data
                        st.dataframe(chat['results'], use_container_width=True)
                        
                        # Show visualization if available
                        if chat['visualization']:
                            st.plotly_chart(chat['visualization'], use_container_width=True)
                        
                        # Export options
                        st.download_button(
                            "📊 Download as CSV",
                            data=self.bot.export_results(chat['results'], 'csv'),
                            file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            key=f"download_{i}"
                        )
                    else:
                        st.error("No results found for this query.")
    
    def process_business_query(self, user_query: str):
        """Process user query and display results"""
        with st.spinner("🤖 Analyzing your question..."):
            try:
                # Convert to SQL
                sql_query = self.bot.interpret_business_query(user_query)
                
                st.code(sql_query, language='sql')
                
                # Execute query
                results_df = self.bot.execute_query(sql_query)
                
                if not results_df.empty:
                    # Generate insights
                    insights = self.bot.generate_business_insights(results_df, user_query)
                    
                    # Create visualization
                    chart = self.bot.create_visualization(results_df, user_query)
                    
                    # Store in chat history
                    st.session_state.chat_history.append({
                        'query': user_query,
                        'sql': sql_query,
                        'results': results_df,
                        'insights': insights,
                        'visualization': chart,
                        'timestamp': datetime.now()
                    })
                    
                    # Display results
                    st.success("✅ Query executed successfully!")
                    st.markdown(f"**Insights:** {insights}")
                    
                    # Show results table
                    st.dataframe(results_df, use_container_width=True)
                    
                    # Show chart if available
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    
                    # Export options
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.download_button(
                            "📊 Download CSV",
                            data=self.bot.export_results(results_df, 'csv'),
                            file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    
                    with col2:
                        st.download_button(
                            "📈 Download JSON",
                            data=self.bot.export_results(results_df, 'json'),
                            file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    
                else:
                    st.warning("No results found for your query. Try rephrasing your question.")
                    
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")
    
    def run_app(self):
        """Run the main application"""
        
        # Main interface tabs
        tab1, tab2, tab3 = st.tabs(["💬 Chat", "📊 Data Explorer", "⚙️ Settings"])
        
        with tab1:
            self.main_chat_interface()
        
        with tab2:
            self.data_explorer_tab()
        
        with tab3:
            self.settings_tab()
    
    def data_explorer_tab(self):
        """Data exploration interface"""
        st.header("📊 Data Explorer")
        
        # Table selector
        conn = sqlite3.connect(self.bot.db_path)
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
        
        selected_table = st.selectbox("Select Table to Explore:", tables)
        
        if selected_table:
            # Show table preview
            sample_data = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 100", conn)
            st.subheader(f"Sample Data from {selected_table}")
            st.dataframe(sample_data, use_container_width=True)
            
            # Basic statistics for numeric columns
            numeric_cols = sample_data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                st.subheader("📈 Summary Statistics")
                st.dataframe(sample_data[numeric_cols].describe())
        
        conn.close()
    
    def settings_tab(self):
        """Settings and configuration"""
        st.header("⚙️ Settings")
        
        st.markdown("### 🔌 Data Connections")
        st.info("Currently using demo SQLite database. In production, you can connect to:")
        
        connection_types = [
            "🗃️ SQLite Database",
            "🐘 PostgreSQL", 
            "🟦 MySQL",
            "📊 Excel Files",
            "📄 CSV Files",
            "☁️ Google Sheets",
            "🛒 Shopify API",
            "📈 Google Analytics",
            "💰 Stripe API"
        ]
        
        for conn_type in connection_types:
            st.markdown(f"- {conn_type}")
        
        st.markdown("### 🤖 AI Configuration")
        api_key = st.text_input("OpenAI API Key:", type="password", help="Enter your OpenAI API key for enhanced natural language processing")
        
        if api_key:
            st.success("✅ API key configured (would connect to real OpenAI in production)")
        
        st.markdown("### 📊 Export Settings")
        default_format = st.selectbox("Default Export Format:", ["CSV", "Excel", "JSON", "PDF Report"])
        
        st.markdown("### 🔔 Alert Settings") 
        st.checkbox("Email alerts for unusual data patterns")
        st.checkbox("Slack notifications for query results")
        st.number_input("Alert threshold (% change):", min_value=0, max_value=100, value=20)

if __name__ == "__main__":
    app = BusinessIntelligenceChatbotApp()
    app.run_app()