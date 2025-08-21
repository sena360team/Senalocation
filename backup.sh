#!/bin/bash
# backup.sh - สคริปต์สำหรับสำรองระบบ Gemini

# เวลาในรูปแบบ YYYYMMDD-HHMMSS
ts=$(date +%Y%m%d-%H%M%S)

# โฟลเดอร์เก็บ backup
backup_dir="backups"
mkdir -p "$backup_dir"

# สร้างไฟล์ ZIP (ยกเว้นไฟล์ไม่จำเป็น)
zip_file="$backup_dir/gemini-$ts.zip"

echo "🔄 กำลังสร้าง backup: $zip_file"

zip -r "$zip_file" . \
  -x "venv/*" ".git/*" "__pycache__/*" "*.pyc" ".DS_Store" "$backup_dir/*"

if [ $? -eq 0 ]; then
  echo "✅ Backup สำเร็จ -> $zip_file"
  ls -lh "$zip_file"
else
  echo "❌ เกิดข้อผิดพลาดในการ backup"
fi
