# app/Dockerfile

# ใช้ Spark image ที่มี Python ติดตั้งมาแล้ว
FROM bitnami/spark:3.3.0

# ติดตั้ง Python libraries ที่จำเป็น
USER root
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# เปลี่ยนกลับเป็น user ปกติ
USER 1001