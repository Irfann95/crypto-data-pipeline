FROM python:3.10-slim

WORKDIR /app

COPY producer_crypto_scrapping.py .

RUN pip3 install kafka-python && \
    pip3 install requests

CMD ["python", "producer_crypto_scrapping.py"]