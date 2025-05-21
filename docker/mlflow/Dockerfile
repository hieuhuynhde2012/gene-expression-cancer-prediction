FROM python:3.9-slim

# Cài đặt MLflow và các công cụ cần thiết
RUN pip install --no-cache-dir mlflow[extras]==2.11.1

# Tạo thư mục cho artifact lưu trữ
RUN mkdir -p /mlflow/mlruns

# Làm việc trong thư mục mlflow
WORKDIR /mlflow

# Expose port MLflow UI
EXPOSE 5000

# Gán entrypoint mặc định
ENTRYPOINT ["mlflow"]
