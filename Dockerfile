FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# KR-FinBert-SC 센티먼트 모델 미리 다운로드
RUN python -c "\
from transformers import AutoTokenizer, AutoModelForSequenceClassification; \
AutoTokenizer.from_pretrained('snunlp/KR-FinBert-SC'); \
AutoModelForSequenceClassification.from_pretrained('snunlp/KR-FinBert-SC'); \
print('Model downloaded successfully')"

# 소스 복사
COPY app/ ./

# PYTHONPATH 설정
ENV PYTHONPATH=/app

# 기본 명령
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]