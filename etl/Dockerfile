FROM python:3.9-slim

WORKDIR /app

COPY ./scripts /app/scripts

RUN pip install --upgrade pip && \
    pip install requests psycopg2-binary boto3 pandas pyarrow faker

CMD ["python", "/app/scripts/fictitious_data_insert.py"]
