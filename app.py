import chainlit as cl
from google.cloud import bigquery
import os
import requests
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
client = bigquery.Client()

# BigQuery configuration
PROJECT_ID = "thelook-459020"
DATASET_ID = "thelook"

# Vanna configuration
VANNA_API_KEY = os.getenv("VANNA_API_KEY")
VANNA_API_URL = "https://app.vanna.ai/api/v2/chat_sse"
VANNA_AGENT_ID = os.getenv("VANNA_AGENT_ID")
VANNA_USER_EMAIL = os.getenv("VANNA_USER_EMAIL")

# Button configurations
BUTTONS = [
    {
        "name": "total_sales",
        "label": "💰 Total Sales",
        "description": "View total revenue and order count"
    },
    {
        "name": "recent_orders",
        "label": "📦 Recent Orders",
        "description": "View the last 10 orders"
    }
]

async def send_action_buttons():
    """Helper function to send action buttons"""
    actions = [
        cl.Action(
            name=btn["name"],
            value=btn["name"],
            label=btn["label"],
            description=btn["description"],
            payload={"action": btn["name"]}
        )
        for btn in BUTTONS
    ]
    await cl.Message(content="What else would you like to know?", actions=actions).send()

def query_bigquery(sql):
    """Execute a BigQuery query and return results"""
    try:
        query_job = client.query(sql)
        results = query_job.result()
        return list(results)
    except Exception as e:
        return f"Error: {str(e)}"

def query_vanna(user_message):
    """Query Vanna AI for natural language to SQL conversion"""
    headers = {
        "Content-Type": "application/json",
        "VANNA-API-KEY": VANNA_API_KEY
    }
    
    payload = {
        "message": user_message,
        "user_email": VANNA_USER_EMAIL,
        "agent_id": VANNA_AGENT_ID,
        "acceptable_responses": ["text", "dataframe"]
    }
    
    try:
        response = requests.post(
            VANNA_API_URL,
            headers=headers,
            json=payload,
            stream=True
        )
        
        # Parse SSE stream
        final_response = ""
        sql_query = None
        dataframe_data = None
        
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    data_str = line_str[6:]  # Remove 'data: ' prefix
                    try:
                        data = json.loads(data_str)
                        
                        # Only capture the final AI message
                        if data.get('type') == 'text' and data.get('semantic_type') == 'final_ai_message':
                            final_response = data.get('text', '')
                        
                        # Extract SQL query if present
                        if 'sql' in data:
                            sql_query = data['sql']
                        
                        # Extract dataframe from json_table format
                        if data.get('type') == 'dataframe' and 'json_table' in data:
                            dataframe_data = data['json_table']['data']
                            
                    except json.JSONDecodeError:
                        continue
        
        return {
            "response": final_response,
            "sql": sql_query,
            "dataframe": dataframe_data
        }
        
    except Exception as e:
        return {"error": str(e)}

@cl.on_chat_start
async def start():
    """Initialize the chat with welcome message and buttons"""
    actions = [
        cl.Action(
            name=btn["name"],
            value=btn["name"],
            label=btn["label"],
            description=btn["description"],
            payload={"action": btn["name"]}
        )
        for btn in BUTTONS
    ]
    
    await cl.Message(
        content="👋 **Welcome to TheLook E-commerce Analytics!**\n\nClick a button below to get instant insights, or type any question about your data:",
        actions=actions
    ).send()

@cl.action_callback("total_sales")
async def on_total_sales(action: cl.Action):
    """Handle Total Sales button click"""
    
    # Show loading message
    msg = cl.Message(content="🔄 Calculating total sales...")
    await msg.send()
    
    # Query BigQuery
    sql = f"""
    SELECT 
        ROUND(SUM(sale_price), 2) as total_sales,
        COUNT(*) as total_orders
    FROM 
        `{PROJECT_ID}.{DATASET_ID}.order_items`
    WHERE 
        sale_price IS NOT NULL
    """
    
    results = query_bigquery(sql)
    
    # Format and display results
    if isinstance(results, str):  # Error case
        msg.content = f"❌ {results}"
        await msg.update()
    else:
        row = results[0]
        total_sales = f"${row.total_sales:,.2f}"
        total_orders = f"{row.total_orders:,}"
        
        response = f"""
💰 **Total Sales Report**

📈 Total Revenue: **{total_sales}**
📦 Total Orders: **{total_orders}**
        """
        
        msg.content = response
        await msg.update()
    
    # Re-display buttons
    await send_action_buttons()

@cl.action_callback("recent_orders")
async def on_recent_orders(action: cl.Action):
    """Handle Recent Orders button click"""
    msg = cl.Message(content="🔄 Loading recent orders...")
    await msg.send()
    
    sql = f"""
    SELECT order_id, user_id, status, num_of_item,
           FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', created_at) as order_time
    FROM `{PROJECT_ID}.{DATASET_ID}.orders`
    ORDER BY created_at DESC
    LIMIT 10
    """
    
    results = query_bigquery(sql)
    
    if isinstance(results, str):
        msg.content = f"❌ {results}"
    else:
        response = "📦 **Last 10 Orders**\n\n"
        for row in results:
            response += f"**Order #{row.order_id}** | {row.status} | {row.num_of_item} items | {row.order_time}\n"
        msg.content = response
    
    await msg.update()
    await send_action_buttons()

@cl.on_message
async def main(message: cl.Message):
    """Handle text input for natural language queries via Vanna"""
    
    # Show thinking message
    thinking_msg = cl.Message(content="🤔 Processing your question with AI...")
    await thinking_msg.send()
    
    # Query Vanna in a separate thread to avoid blocking the async event loop
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        vanna_result = await loop.run_in_executor(executor, query_vanna, message.content)
    
    if "error" in vanna_result:
        thinking_msg.content = f"❌ Error: {vanna_result['error']}"
        await thinking_msg.update()
        return
    
    # Update with the AI response
    if vanna_result.get("response"):
        thinking_msg.content = vanna_result["response"]
        await thinking_msg.update()
    else:
        thinking_msg.content = "No response received from AI. Please try again."
        await thinking_msg.update()
    
    # Re-display buttons
    await send_action_buttons()