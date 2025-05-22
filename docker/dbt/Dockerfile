FROM python:3.9-slim

# Cài đặt các dependencies hệ thống
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && apt-get clean

# Giáng cấp protobuf xuống bản ổn định (3.20.x)
RUN pip install --no-cache-dir "protobuf<4,>=3.20.0"

# Cài đặt dbt-core và adapter cho PostgreSQL
RUN pip install --no-cache-dir dbt-postgres==1.7.4

# Tạo thư mục làm việc
WORKDIR /app/dwh

# Gán CMD mặc định
CMD ["dbt", "--help"]
