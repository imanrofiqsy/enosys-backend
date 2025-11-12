FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway 
ENV PORT=8000

# dummy_data dan Daphne
# CMD ["sh", "-c", "python manage.py send_data & python manage.py polling_influx & daphne -b 0.0.0.0 -p ${PORT} api.asgi:application"]
CMD ["sh", "-c", "python manage.py polling_influx & daphne -b 0.0.0.0 -p ${PORT} api.asgi:application"]
