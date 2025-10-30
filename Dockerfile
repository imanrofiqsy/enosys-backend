# Gunakan image Python resmi (lebih ringan gunakan slim)
FROM python:3.11-slim

# Set working directory di dalam container
WORKDIR /app

# Salin file requirements lebih dulu (agar cache build efisien)
COPY requirements.txt .

# Install dependensi
RUN pip install --no-cache-dir -r requirements.txt

# Salin seluruh source code proyek
COPY . .

# Railway otomatis memberi env $PORT, kita ambil nilainya untuk Daphne
# Gunakan entrypoint agar bisa dijalankan dengan port dinamis
CMD ["sh", "-c", "daphne -b 0.0.0.0 -p ${PORT:-8000} api.asgi:application"]