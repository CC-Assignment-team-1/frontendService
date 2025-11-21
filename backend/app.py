import os
import boto3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Backend API")

# CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DynamoDB setup
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION', 'ap-south-1')
)

TABLE_NAME = os.getenv('DDB_TABLE_NAME', 'kafka_messages')
table = dynamodb.Table(TABLE_NAME)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok"}


@app.get("/api/data")
async def get_data(limit: int = 50):
    """Fetch data from DynamoDB"""
    try:
        response = table.scan(Limit=limit)
        items = response.get('Items', [])
        return {
            "status": "success",
            "count": len(items),
            "data": items
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
