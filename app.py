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
# Check if GOOGLE_CREDENTIALS env var exists (for Railway deployment)
if os.getenv("GOOGLE_CREDENTIALS"):
    # Write credentials to temp file
    import tempfile
    credentials_path = os.path.join(tempfile.gettempdir(), "credentials.json")
    with open(credentials_path, 'w') as f:
        f.write(os.getenv("GOOGLE_CREDENTIALS"))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
else:
    # Use local service account file
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
💰 **Total Sales Report (All Time)**

📈 Total Revenue: **{total_sales}**
📦 Total Orders: **{total_orders}**
        """
        
        msg.content = response
        await msg.update()
    
    # Show time period follow-up buttons
    time_actions = [
        cl.Action(
            name="sales_1week",
            value="sales_1week",
            label="📅 Last Week",
            description="Sales from last 7 days",
            payload={"action": "sales_1week"}
        ),
        cl.Action(
            name="sales_1month",
            value="sales_1month",
            label="📅 Last Month",
            description="Sales from last 30 days",
            payload={"action": "sales_1month"}
        ),
        cl.Action(
            name="sales_6months",
            value="sales_6months",
            label="📅 Last 6 Months",
            description="Sales from last 180 days",
            payload={"action": "sales_6months"}
        )
    ]
    await cl.Message(content="View sales for a specific time period:", actions=time_actions).send()
    
    # Also re-display main buttons
    await send_action_buttons()

@cl.action_callback("sales_1week")
async def on_sales_1week(action: cl.Action):
    """Handle Last Week sales button"""
    msg = cl.Message(content="🔄 Calculating sales for last week of data...")
    await msg.send()
    
    sql = f"""
    SELECT 
        ROUND(SUM(sale_price), 2) as total_sales,
        COUNT(*) as total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.order_items`
    WHERE sale_price IS NOT NULL
      AND created_at >= '2025-05-19'
      AND created_at < '2025-05-26'
    """
    
    results = query_bigquery(sql)
    
    if isinstance(results, str):
        msg.content = f"❌ {results}"
    else:
        row = results[0]
        response = f"""
📅 **Sales Report - Week of May 19-25, 2025**

📈 Total Revenue: **${row.total_sales:,.2f}**
📦 Total Orders: **{row.total_orders:,}**
        """
        msg.content = response
    
    await msg.update()
    await send_action_buttons()

@cl.action_callback("sales_1month")
async def on_sales_1month(action: cl.Action):
    """Handle Last Month sales button"""
    msg = cl.Message(content="🔄 Calculating sales for last month of data...")
    await msg.send()
    
    sql = f"""
    SELECT 
        ROUND(SUM(sale_price), 2) as total_sales,
        COUNT(*) as total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.order_items`
    WHERE sale_price IS NOT NULL
      AND created_at >= '2025-04-26'
      AND created_at < '2025-05-26'
    """
    
    results = query_bigquery(sql)
    
    if isinstance(results, str):
        msg.content = f"❌ {results}"
    else:
        row = results[0]
        response = f"""
📅 **Sales Report - April 26 - May 25, 2025**

📈 Total Revenue: **${row.total_sales:,.2f}**
📦 Total Orders: **{row.total_orders:,}**
        """
        msg.content = response
    
    await msg.update()
    await send_action_buttons()

@cl.action_callback("sales_6months")
async def on_sales_6months(action: cl.Action):
    """Handle Last 6 Months sales button"""
    msg = cl.Message(content="🔄 Calculating sales for last 6 months of data...")
    await msg.send()
    
    sql = f"""
    SELECT 
        ROUND(SUM(sale_price), 2) as total_sales,
        COUNT(*) as total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.order_items`
    WHERE sale_price IS NOT NULL
      AND created_at >= '2024-11-26'
      AND created_at < '2025-05-26'
    """
    
    results = query_bigquery(sql)
    
    if isinstance(results, str):
        msg.content = f"❌ {results}"
    else:
        row = results[0]
        response = f"""
📅 **Sales Report - Nov 2024 - May 2025 (6 months)**

📈 Total Revenue: **${row.total_sales:,.2f}**
📦 Total Orders: **{row.total_orders:,}**
        """
        msg.content = response
    
    await msg.update()
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
    await cl.Message(content="🤔 Processing your question with AI...").send()
    
    # Create an async queue to communicate between threads
    import queue
    msg_queue = queue.Queue()
    
    # Stream Vanna responses in a thread
    def get_vanna_stream():
        response = requests.post(
            VANNA_API_URL,
            headers={
                "Content-Type": "application/json",
                "VANNA-API-KEY": VANNA_API_KEY
            },
            json={
                "message": message.content,
                "user_email": VANNA_USER_EMAIL,
                "agent_id": VANNA_AGENT_ID,
                "acceptable_responses": ["text", "dataframe"]
            },
            stream=True
        )
        
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    data_str = line_str[6:]
                    try:
                        data = json.loads(data_str)
                        
                        # Send intermediate messages to queue
                        if data.get('type') == 'text' and data.get('semantic_type') == 'intermediate_ai_message':
                            msg_queue.put(('intermediate', data.get('text', '')))
                        
                        # Send final response to queue
                        if data.get('type') == 'text' and data.get('semantic_type') == 'final_ai_message':
                            msg_queue.put(('final', data.get('text', '')))
                            
                    except json.JSONDecodeError:
                        continue
        
        # Signal completion
        msg_queue.put(('done', None))
    
    # Start the Vanna thread
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()
    loop.run_in_executor(executor, get_vanna_stream)
    
    # Process messages as they arrive
    final_text = None
    while True:
        try:
            # Check queue with timeout
            msg_type, text = await loop.run_in_executor(executor, msg_queue.get, True, 0.1)
            
            if msg_type == 'done':
                break
            elif msg_type == 'intermediate':
                await cl.Message(content=f"💭 {text}").send()
            elif msg_type == 'final':
                final_text = text
                
        except queue.Empty:
            continue
    
    # Display final answer
    if final_text:
        await cl.Message(content=f"✅ **Final Answer:**\n\n{final_text}").send()
    else:
        await cl.Message(content="No response received from AI. Please try again.").send()
    
    # Re-display buttons
    await send_action_buttons()