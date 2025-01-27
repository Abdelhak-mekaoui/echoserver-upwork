FROM python:3.9-slim as builder

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.9-slim

WORKDIR /app

COPY --from=builder /install /usr/local

COPY . /app

EXPOSE 32222

CMD ["python", "main.py"]