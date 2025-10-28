# Gunakan image Python resmi
FROM python:3.11

# Set working directory di dalam container
WORKDIR /app

# Salin semua file project ke dalam container
COPY . .

# Install dependensi
RUN pip install --no-cache-dir -r requirements.txt

# Jalankan Daphne untuk ASGI (Django Channels)
CMD ["daphne", "-b", "0.0.0.0", "-p", "8000", "api.asgi:application"]