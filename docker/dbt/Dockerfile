FROM python:3.9-slim

# Cài đặt các dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && apt-get clean

# Cài đặt dbt-core và adapter cho PostgreSQL
RUN pip install --no-cache-dir dbt-postgres==1.7.4

# Tạo thư mục làm việc
WORKDIR /app/dwh

# Gán entrypoint mặc định
ENTRYPOINT ["dbt"]
