from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uvicorn

from grok import GrokClient

app = FastAPI()

# Request models
class CreateConversationRequest(BaseModel):
    """
    Request model for creating a new Grok conversation
    
    Fields:
    - auth_token: X/Twitter authentication token (required)
    - ct0: CSRF token (required)
    - proxy: Optional proxy URL (e.g., "http://proxy:port")
    """
    auth_token: str
    ct0: str
    proxy: Optional[str] = None

class SendMessageRequest(BaseModel):
    """
    Request model for sending messages to Grok
    
    Fields:
    - auth_token: X/Twitter authentication token (required)
    - ct0: CSRF token (required)
    - message: Text to send to Grok (required)
    - conversation_id: Existing conversation ID (optional - creates new if empty)
    - model: Grok model version (default: "grok-3")
    - proxy: Optional proxy URL
    - raw_response: Include raw API response (default: False)
    """
    auth_token: str
    ct0: str
    message: str
    conversation_id: Optional[str] = ""
    model: Optional[str] = "grok-3"
    proxy: Optional[str] = None
    raw_response: Optional[bool] = False

class GetHistoryRequest(BaseModel):
    """
    Request model for retrieving conversation history
    
    Fields:
    - auth_token: X/Twitter authentication token (required)
    - ct0: CSRF token (required)
    - conversation_id: Conversation ID to retrieve (required)
    - proxy: Optional proxy URL
    """
    auth_token: str
    ct0: str
    conversation_id: str
    proxy: Optional[str] = None

# Response models
class GrokResponse(BaseModel):
    """
    Standard API response format
    
    Fields:
    - success: Boolean indicating operation status
    - data: Response payload (varies by endpoint)
    - error: Error message if success=False
    """
    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None

def get_client(auth_token: str, ct0: str, proxy: Optional[str] = None) -> GrokClient:
    """Internal helper to initialize authenticated GrokClient"""
    return GrokClient(auth_token=auth_token, ct0=ct0, proxy=proxy, max_retries=10)

@app.post(
    "/create_conversation",
    response_model=GrokResponse,
    summary="Create New Conversation",
    response_description="Returns new conversation ID",
    tags=["Conversations"]
)
async def create_conversation(data: CreateConversationRequest):
    """
    Create a new Grok conversation
    
    ## Example Request
    ```json
    {
        "auth_token": "your_auth_token",
        "ct0": "your_ct0_token"
    }
    ```
    
    ## Example Response
    ```json
    {
        "success": true,
        "data": {
            "data": {...},
            "conversation_id": "1923295392357069084"
        }
    }
    ```
    """
    try:
        with get_client(data.auth_token, data.ct0, data.proxy) as client:
            r = client.create_conversation()
            data = r.get("data", {})
            return GrokResponse(
                success=True,
                data={
                    "data": data,
                    "conversation_id": data.get("create_grok_conversation", {}).get("conversation_id")
                },
                error=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post(
    "/send_message",
    response_model=GrokResponse,
    summary="Send Message",
    response_description="Returns Grok's response",
    tags=["Messaging"]
)
async def send_message(data: SendMessageRequest):
    """Send a message to Grok (creates new conversation if none specified)
    
    ## Features
    - Auto-creates conversation when no ID provided
    - Streams and parses response automatically
    - Optional raw response output
    
    ## Example Request
    ```json
    {
        "auth_token": "your_auth_token",
        "ct0": "your_ct0_token",
        "message": "Hello Grok!"
    }
    ```
    
    ## Example Response
    ```json
    {
        "success": true,
        "data": {
            "clean_response": "Hey there! How can I help?",
            "raw_response": "{...}"  // Only if raw_response=true
        }
    }
    """
    try:
        with get_client(data.auth_token, data.ct0, data.proxy) as client:
            conversation_id  = data.conversation_id
            if not data.conversation_id:
                conversation_id = client.create_conversation().get("data", {}).get("create_grok_conversation", {}).get("conversation_id")
            raw_response = client.send_message(
                message=data.message,
                conversation_id=conversation_id,
                model=data.model
            )
            clean_response = client.parse_grok_response(raw_response)
            return GrokResponse(
                success=True,
                data={
                    "raw_response": raw_response if data.raw_response else None,
                    "clean_response": clean_response
                },
                error=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/conversation_history",
    response_model=GrokResponse,
    summary="Get Conversation History",
    response_description="Returns full message history",
    tags=["Conversations"]
)
async def get_conversation_history(auth_token: str, ct0: str, conversation_id: str, proxy: Optional[str] = None):
    """Retrieve complete message history for a conversation
    
    ## Parameters
    - `auth_token`: Required in query string
    - `ct0`: Required in query string
    - `conversation_id`: Required conversation ID
    
    ## Example Request
    ```
    GET /conversation_history?auth_token=XXX&ct0=YYY&conversation_id=1923295392357069084
    ```
    
    ## Example Response
    ```json
    {
        "success": true,
        "data": {...}
    }
    """
    try:
        with get_client(auth_token, ct0, proxy) as client:
            r = client.get_conversation_history(conversation_id)
            return GrokResponse(
                success=True,
                data=r,
                error=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/generate_transaction_id",
    response_model=GrokResponse,
    summary="Generate X-Client-Transaction ID",
    response_description="Returns generated transaction ID",
    tags=["Utilities"]
)
async def generate_transaction_id(
    auth_token: str,
    ct0: str,
    method: str,
    path: str,
    proxy: Optional[str] = None
):
    """Generate an X-Client-Transaction ID for API requests (GET version)
    
    ## Parameters
    - `auth_token`: X/Twitter authentication token (required in query)
    - `ct0`: CSRF token (required in query)
    - `method`: HTTP method (e.g., "GET", "POST") (required)
    - `path`: URL path component (e.g., "/api/grok") (required)
    - `proxy`: Optional proxy URL
    
    ## Example Request
    ```
    GET /generate_transaction_id?auth_token=XXX&ct0=YYY&method=POST&path=/api/grok
    ```
    
    ## Example Response
    ```json
    {
        "success": true,
        "data": {
            "transaction_id": "transaction_id"
        }
    }
    ```
    """
    try:
        with get_client(auth_token, ct0, proxy) as client:
            transaction_id = client.ct.generate_transaction_id(
                method=method,
                path=path
            )
            return GrokResponse(
                success=True,
                data={"transaction_id": transaction_id},
                error=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"success": True, "status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
