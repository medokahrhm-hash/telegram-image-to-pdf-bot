# استخدام نسخة بايثون مستقرة
FROM python:3.9

# تحديد مجلد العمل داخل السيرفر
WORKDIR /code

# نسخ ملفات البوت من GitHub إلى السيرفر
COPY . .

# تثبيت المكتبات من ملف requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# الأمر النهائي لتشغيل البوت
CMD ["python", "main.py"]
