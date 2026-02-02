from fastapi import FastAPI
from app.api.endpoints import stocks

app = FastAPI(title="Financial AI Service")

app.include_router(stocks.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Finance AI Server is Running!"}

@app.get("/test")
def predict(ticker: str):
    return {"ticker": ticker, "prediction": "Wait for AI model..."}