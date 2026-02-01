# app/main.py
from fastapi import FastAPI

app = FastAPI(title="Financial AI Service")

@app.get("/")
def read_root():
    return {"message": "Finance AI Server is Running!"}

@app.get("/test")
def predict(ticker: str):
    return {"ticker": ticker, "prediction": "Wait for AI model..."}