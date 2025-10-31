FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway kasih port otomatis (8080 misalnya)
ENV PORT=8000

# Jalankan dummy_data dan Daphne bersamaan
CMD ["sh", "-c", "python dummy_data.py & daphne -b 0.0.0.0 -p ${PORT} api.asgi:application"]
#CMD ["sh", "-c", "daphne -b 0.0.0.0 -p ${PORT} api.asgi:application"]
