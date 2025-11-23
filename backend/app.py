import os
import boto3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AQI Analytics API")

# CORS middleware - restrict to specific origins
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
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
    """
    Fetch latest AQI aggregated results from DynamoDB.
    Returns newest entries first (sorted by timestamp descending).
    """
    try:
        logger.info(f"Fetching latest {limit} AQI records")
        
        # Scan all items and sort by timestamp to get latest entries
        response = table.scan()
        items = response.get('Items', [])
        
        # Sort by timestamp in descending order (latest first)
        items.sort(
            key=lambda x: x.get('timestamp', ''),
            reverse=True
        )
        
        # Get only the latest 'limit' entries
        items = items[:limit]
        
        logger.info(f"Successfully fetched {len(items)} records")
        
        return {
            "status": "success",
            "count": len(items),
            "data": items
        }
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


@app.get("/api/data/stats")
async def get_stats():
    """Get statistics about AQI values"""
    try:
        logger.info("Calculating AQI statistics")
        
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            return {
                "status": "success",
                "total_records": 0,
                "message": "No data available"
            }
        
        values = [float(item.get('random_number', 0)) for item in items]
        values.sort()
        
        total = len(values)
        mean = sum(values) / total
        median = values[total // 2] if total % 2 == 1 else (values[total // 2 - 1] + values[total // 2]) / 2
        
        # Calculate percentiles
        p25 = values[int(total * 0.25)]
        p75 = values[int(total * 0.75)]
        
        stats = {
            "status": "success",
            "total_records": total,
            "min": min(values),
            "max": max(values),
            "mean": round(mean, 2),
            "median": round(median, 2),
            "p25": round(p25, 2),
            "p75": round(p75, 2),
            "std_dev": round((sum((x - mean) ** 2 for x in values) / total) ** 0.5, 2)
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error calculating stats: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
