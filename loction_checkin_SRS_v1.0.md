# SRS — LINE Field Ops Bot (v1.0)

## 1) ขอบเขต (Scope)
ระบบช่วยงานภาคสนามผ่าน LINE: ลงทะเบียนผู้ใช้, เช็คอินหน้างานด้วยตำแหน่ง+รูป, ส่งงานพร้อมรูป, จัดเก็บข้อมูลลง Google Sheets/Drive, มีตัวเตือนหมดเวลา และตรวจรูปซ้ำสำหรับ “ส่งงาน”.

## 2) ผู้มีส่วนเกี่ยวข้อง (Actors)
- พนักงานภาคสนาม (LINE user)
- หัวหน้างาน/แอดมิน (ดูข้อมูลใน Google Sheets/Drive)
- ระบบภายนอก (Google Sheets, Google Drive)

## 3) คุณลักษณะหลัก (Features)
### 3.1 ลงทะเบียน (Registration)
- เริ่มด้วยคำว่า “ลงทะเบียน / register / สมัคร”.
- เก็บ: ชื่อ–นามสกุล → ให้เลือกตำแหน่งงานจาก Quick Reply ดึงสดจากชีต **Roles**.
- จบการลงทะเบียนด้วยข้อความสรุป “ชื่อ/ตำแหน่ง” โดย **ไม่** แสดงปุ่มให้เช็คอิน/ส่งงานต่อทันที (ผู้ใช้ไปกดจาก Rich Menu เอง).

### 3.2 เช็คอิน (Check-in)
- คำสั่งเริ่ม: “เช็คอิน / checkin”.
- ระบบสร้าง **transaction_id** และให้เปิด LIFF เพื่อส่ง **location** (แนบเมทา `(txn|acc|ts)`).
- ตรวจความสดของเวลา (ts ภายใน MAX_LOCATION_AGE_SEC) และความแม่นยำ (acc ≤ MAX_GPS_ACCURACY_M).
- จับคู่สถานที่จากชีต **Locations** ด้วย **checkin_radius_meters**.
- อัปโหลดรูปได้สูงสุด 3 รูป (บีบอัดตาม IMAGE_QUALITY_CHECKIN).
- สถานะ: `pending → in_progress → done/timeout/cancelled`.
- **การปิดงาน**: ผู้ใช้พิมพ์ “จบ/จบการเช็คอิน” (ไม่มี auto-close).
- ตัวตั้งเวลา (Scheduler) เตือนก่อนหมดเวลาและปิดอัตโนมัติเมื่อเกิน **CHECKIN_TIMEOUT_SECONDS**.

### 3.3 ส่งงาน (Submission)
- คำสั่งเริ่ม: “ส่งงาน / submit”.
- ขั้นตอนเหมือนเช็คอินแต่ใช้ **submission_radius_meters** สำหรับจับคู่สถานที่.
- อัปโหลดรูปสูงสุด 3 รูป (บีบอัดตาม IMAGE_QUALITY_SUBMISSION).
- บันทึกค่าแฮชภาพ (aHash) ลงคอลัมน์ M..O และถ้าพบ “ภาพซ้ำกับงานก่อนหน้า” จะบันทึกอ้างอิงที่คอลัมน์ P..R **โดยไม่แจ้งเตือนตำแหน่งแถว/คอลัมน์ใน LINE**.
- **Auto-finalize**: เมื่อครบ 3 รูป ระบบปิดงานเป็น `done` อัตโนมัติ และแจ้ง “ส่งงานเรียบร้อย”.

### 3.4 LIFF Location + Anti-fraud
- หน้า LIFF อ่านพิกัดและส่งเข้าแชตพร้อม `(txn|acc|ts)`.
- เปิดใน LINE client เท่านั้น (เตือนถ้าเปิดนอกแอป).
- ฝั่งเซิร์ฟเวอร์ยอมรับตำแหน่งเมื่อ **ts ยังสด** และ **acc ไม่เกินเกณฑ์**.

### 3.5 ความทนทาน/ประสิทธิภาพ
- เรียก Google Sheets ด้วย timeout + retry/backoff.
- Cache ชีต Employees/Roles ตาม TTL.
- ล็อกธุรกรรมต่อรายการ (per-transaction lock) ป้องกันเขียนทับเมื่อภาพเข้าซ้อนกัน.

## 4) สถานะการทำงาน (State Machine)
**Registration**: `idle → awaiting_registration_name → awaiting_registration_role → idle`

**Check-in**: `idle → waiting_for_checkin_location → waiting_for_checkin_images → done/timeout/cancelled`

**Submission**: `idle → waiting_for_submit_location → waiting_for_submit_images → done/cancelled` *(auto-finalize เมื่อครบ 3 รูป)*

## 5) โครงสร้างชีต (Google Sheets)

### 5.1 Employees
| คอลัมน์ | ชื่อฟิลด์ | ชนิด | บังคับ | อธิบาย |
|---|---|---|---|---|
| A | line_user_id | String | ✔ | LINE userId (คีย์หลัก) |
| B | employee_name | String |  | ชื่อ–นามสกุล |
| C | role | String |  | ตำแหน่งงาน (จาก Roles หรือพิมพ์เอง) |
| D | current_state | Enum | ✔ | สถานะปัจจุบัน (เช่น `idle`, `awaiting_registration_name`, …) |
| E | current_transaction_id | String |  | ผูกกับรายการเช็คอิน/ส่งงานที่ค้าง |

### 5.2 CheckIns
| คอลัมน์ | ฟิลด์ | ชนิด | บังคับ | อธิบาย |
|---|---|---|---|---|
| A | checkin_id | UUID | ✔ | ไอดีธุรกรรมเช็คอิน |
| B | created_at | Datetime | ✔ | เวลาสร้าง |
| C | line_user_id | String | ✔ | อ้างถึงพนักงาน |
| D | location_name | String | ✔ | ชื่อไซต์ที่จับคู่ได้/Lat,Lon |
| E | site_group | String |  | กลุ่มไซต์ |
| F | image_url_1 | URL |  | ลิงก์รูป 1 |
| G | image_url_2 | URL |  | ลิงก์รูป 2 |
| H | image_url_3 | URL |  | ลิงก์รูป 3 |
| I | last_updated_at | Datetime | ✔ | อัปเดตล่าสุด |
| J | status | Enum | ✔ | `pending/in_progress/warning/done/timeout/cancelled` |
| K | warning_sent | Bool/String |  | เคยเตือนใกล้หมดเวลาหรือยัง |
| L | distance_m | Number |  | ระยะจากจุดอ้างอิง (เมตร) |
| M | employee_name | String |  | สำเนาชื่อเพื่อดูรายงานเร็ว |

### 5.3 Submissions
| คอลัมน์ | ฟิลด์ | ชนิด | บังคับ | อธิบาย |
|---|---|---|---|---|
| A | submit_id | UUID | ✔ | ไอดีธุรกรรมส่งงาน |
| B | created_at | Datetime | ✔ | เวลาสร้าง |
| C | line_user_id | String | ✔ | อ้างถึงพนักงาน |
| D | location_name | String | ✔ | ชื่อไซต์/Lat,Lon |
| E | site_group | String |  | กลุ่มไซต์ |
| F | image_url_1 | URL |  | รูป 1 |
| G | image_url_2 | URL |  | รูป 2 |
| H | image_url_3 | URL |  | รูป 3 |
| I | last_updated_at | Datetime | ✔ | อัปเดตล่าสุด |
| J | status | Enum | ✔ | `pending/in_progress/done/cancelled` *(auto→done เมื่อครบ 3 รูป)* |
| K | warning_sent |  |  | เว้นไว้ (สอดคล้องเช็คอิน) |
| L | distance_m | Number |  | ระยะจากจุดอ้างอิง |
| M | image_hash_1 | String |  | aHash รูป 1 |
| N | image_hash_2 | String |  | aHash รูป 2 |
| O | image_hash_3 | String |  | aHash รูป 3 |
| P | duplicate_of_1 | String |  | อ้างอิงรายการที่ซ้ำ (เช่น “row 12 col F”) |
| Q | duplicate_of_2 | String |  | เช่นเดียวกัน |
| R | duplicate_of_3 | String |  | เช่นเดียวกัน |
| S | employee_name | String |  | สำเนาชื่อเพื่อดูรายงานเร็ว |

### 5.4 Locations
| คอลัมน์ | ฟิลด์ | ชนิด | บังคับ | อธิบาย |
|---|---|---|---|---|
| A | location_name | String | ✔ | ชื่อสถานที่ |
| B | site_group | String |  | กลุ่มสถานที่/แผนก |
| C | latitude | Number | ✔ | ละติจูด |
| D | longitude | Number | ✔ | ลองจิจูด |
| E | checkin_radius_meters | Number | ✔ | รัศมีจับคู่สำหรับเช็คอิน |
| F | submission_radius_meters | Number | ✔ | รัศมีจับคู่สำหรับส่งงาน |

### 5.5 Roles
| คอลัมน์ | ฟิลด์ | ชนิด | บังคับ | อธิบาย |
|---|---|---|---|---|
| A | key | String |  | รหัสสั้น (ออปชัน) |
| B | display_name | String | ✔ | ชื่อที่แสดงใน Quick Reply |
| C | sort_order | Number |  | ลำดับแสดงผล |
| D | active | Bool/String |  | เปิด/ปิดการใช้งาน |

> ระบบจะอ่าน **display_name** (คอลัมน์ B) ก่อน ถ้าไม่มีจึง fallback ไปใช้คอลัมน์ A

## 6) กติกา/ตรรกะธุรกิจ (Business Rules)
- รูปสูงสุด 3 รูป/รายการ; ถ้ารูปครบแล้ว เพิ่มเติมจะไม่ทับช่องเดิม.
- “ส่งงาน”: ครบ 3 รูป → **Auto-finalize เป็น `done`** และแจ้งข้อความสำเร็จ.
- “เช็คอิน”: ไม่ auto-finalize; ให้ผู้ใช้พิมพ์ “จบ/จบการเช็คอิน” หรือปล่อยให้ timeout.
- การตรวจรูปซ้ำทำเฉพาะ “ส่งงาน”: เก็บบันทึกในชีต (P..R) **ไม่** แจ้งรายละเอียดใน LINE.
- นโยบายจับคู่สถานที่เมื่ออยู่นอกรัศมี: `SITE_NO_MATCH_POLICY` (ค่าเริ่มต้น `nearest_or_coords`).
- ป้องกันกดซ้ำ/ส่งซ้ำ: เดดุป event id และล็อกธุรกรรมต่อรายการ.

## 7) ค่าคงที่/ตั้งค่า (Configuration via .env)

ค่าในไฟล์ `.env` ใช้กำหนดพฤติกรรมของระบบแบบไม่ต้องแก้โค้ด โดยอ่านเข้ามาเมื่อแอปเริ่มทำงาน (process env). แนะนำให้แยกไฟล์สำหรับแต่ละสภาพแวดล้อม (DEV/STA/PROD) และ **อย่า commit** ขึ้น repo

### 7.1 LINE / Secret
- `LINE_CHANNEL_ACCESS_TOKEN`  
  โทเคนสำหรับเรียก LINE Messaging API (ส่งข้อความ, push, rich menu ฯลฯ) หากไม่ถูกต้องจะได้ 401/403 และบอทจะส่งข้อความไม่ได้
- `LINE_CHANNEL_SECRET`  
  ใช้ตรวจลายเซ็น `X-Line-Signature` เพื่อยืนยันว่า webhook มาจาก LINE จริง ๆ ถ้าผิด ระบบจะปฏิเสธคำขอ

### 7.2 Google (Sheets/Drive/OAuth)
- `GOOGLE_SHEET_ID`  
  ID ของสเปรดชีตหลักที่เก็บแท็บ Employees/CheckIns/Submissions/Locations/Roles เปลี่ยนค่านี้เพื่อสลับฐานข้อมูลระหว่างสภาพแวดล้อม
- `GOOGLE_DRIVE_FOLDER_ID`  
  โฟลเดอร์ปลายทางบน Google Drive ที่เก็บไฟล์รูปจากผู้ใช้ ระบบอัปโหลดไฟล์ลงโฟลเดอร์นี้แล้วตั้งสิทธิ์แชร์แบบลิงก์
- OAuth Client/Secret/Redirect *(ถ้ามีใช้)*  
  กรณีใช้งาน OAuth ฝั่งเว็บ/LIFF ให้กำหนดค่า Client ID/Secret และ Redirect URI ให้ตรงกับที่ตั้งค่าใน Google Cloud Console

### 7.3 LIFF / Anti-fraud (ตำแหน่งที่ตั้ง)
- `LIFF_ID`  
  รหัส LIFF app ที่ใช้เปิดหน้าเก็บพิกัด (location picker)
- `MAX_GPS_ACCURACY_M` *(เช่น 50)*  
  ค่าความคลาดเคลื่อนแนวนอนสูงสุด (เมตร) ที่ระบบยอมรับ ถ้าเกินจะปฏิเสธเพื่อลดความเสี่ยงจากพิกัดไม่แม่น
- `MAX_LOCATION_AGE_SEC` *(เช่น 60)*  
  อายุข้อมูลพิกัดสูงสุดที่ยอมรับ (วินาที) นานเกินนี้ถือว่า “พิกัดเก่า” และจะไม่รับ

### 7.4 Timeout / Scheduler
- `CHECKIN_TIMEOUT_SECONDS`  
  เวลาสูงสุดที่อนุญาตให้รายการเช็คอินเปิดค้างก่อนถูกปิดเป็น `timeout` (ใช้กับ flow เช็คอิน)
- `SCHEDULER_INTERVAL_SECONDS`  
  ความถี่ที่ตัว scheduler ภายในรันเพื่อตรวจแจ้งเตือน/ปิดงานอัตโนมัติ ค่ายิ่งต่ำยิ่งตอบสนองเร็วแต่ใช้ทรัพยากรเพิ่ม
- `APP_TIMEZONE`  
  โซนเวลา IANA (เช่น `Asia/Bangkok`) ใช้แปลง/แสดงเวลาให้ถูกต้อง
- `WARNING_BEFORE_SECONDS`  
  เวลาล่วงหน้าก่อนหมดอายุที่ระบบจะส่งคำเตือนผู้ใช้ สำหรับรายการที่ยังไม่จบ

### 7.5 Sheets Performance / Caching
- `SHEETS_EXECUTE_TIMEOUT_SEC`  
  timeout ระดับ HTTP ต่อคำขอ Sheets (ช่วยกันค้างในเครือข่ายช้า)
- `SHEETS_MAX_ATTEMPTS`  
  จำนวนครั้งสูงสุดในการ retry หากเรียก API ล้มเหลวชั่วคราว
- `SHEETS_BACKOFF_SECONDS`  
  ค่าเริ่มต้นของ backoff ระหว่างการ retry (มักเป็น exponential backoff)
- `EMP_CACHE_TTL_SEC`  
  อายุแคชข้อมูล Employees ในหน่วยวินาที ลดจำนวนครั้งที่ต้องอ่านชีตซ้ำ

### 7.6 Roles (Quick Reply ตำแหน่งงาน)
- `ROLES_SHEET_NAME` *(ค่าเริ่มต้น `Roles`)*  
  ชื่อแท็บที่เก็บรายการตำแหน่งงาน (อ่านมาใช้สร้าง Quick Reply)
- `ROLES_CACHE_TTL_SEC`  
  อายุแคชของรายการ Roles (หมดอายุแล้วจึงอ่านชีตใหม่)

### 7.7 Locations (การจับคู่สถานที่)
- `LOCATIONS_SHEET_NAME`  
  ชื่อแท็บ Locations ที่เก็บจุดอ้างอิง (lat/lon และรัศมีต่อ flow)
- `SITE_NO_MATCH_POLICY`  
  นโยบายเมื่ออยู่นอกรัศมี จับคู่ไม่ได้:  
  - `nearest_or_coords` *(ค่าแนะนำ)* เลือกจุดที่ใกล้สุด หรือ fallback เป็น “พิกัดดิบ”  
  - `reject` ปฏิเสธและให้ผู้ใช้ลองใหม่  
  - `always_coords` ไม่จับคู่ใด ๆ เก็บเป็นพิกัดดิบเสมอ

### 7.8 Submissions
- `SUBMISSIONS_SHEET_NAME`  
  ชื่อแท็บ Submissions (สำหรับบันทึกการส่งงานและ hash/การตรวจรูปซ้ำ)

### 7.9 ภาพ (บีบอัด/ย่อก่อนอัปโหลด)
- `IMAGE_MAX_DIM`  
  ขนาดด้านยาวสูงสุดของภาพ (px) ระบบจะย่อภาพให้ไม่เกินค่านี้เพื่อลดเวลาอัปโหลดและโควตา Drive
- `IMAGE_JPEG_QUALITY`  
  คุณภาพ JPEG พื้นฐาน (0–100) ใช้เมื่อไม่มี override ราย flow
- `IMAGE_QUALITY_CHECKIN`  
  คุณภาพ JPEG สำหรับเช็คอิน (override เฉพาะ flow)
- `IMAGE_QUALITY_SUBMISSION`  
  คุณภาพ JPEG สำหรับส่งงาน (override เฉพาะ flow)

### 7.10 Thread Pool
- `THREAD_POOL_WORKERS`  
  จำนวนเธรดสำหรับงาน I/O ขนาน (อัปโหลด Drive, เรียก Sheets ฯลฯ) ค่ามากขึ้นช่วยเร็วขึ้นแต่กิน CPU/Memory มากขึ้นและอาจชนโควตาเร็ว

---

### ตัวอย่างไฟล์ `.env` (สำหรับ DEV)

```dotenv
# LINE
LINE_CHANNEL_ACCESS_TOKEN=xxxxxxxxxxxxxxxx
LINE_CHANNEL_SECRET=yyyyyyyyyyyyyyyyyyyy

# Google
GOOGLE_SHEET_ID=1ABCdefGhIJklMNopQRstuVWxyz1234567890
GOOGLE_DRIVE_FOLDER_ID=0Bxxxxxxxxxxxxxxxxxxxxxxxx

# LIFF / Anti-fraud
LIFF_ID=165xxxxx-xxxxxxxx
MAX_GPS_ACCURACY_M=50
MAX_LOCATION_AGE_SEC=60

# Timeout / Scheduler
CHECKIN_TIMEOUT_SECONDS=1800
SCHEDULER_INTERVAL_SECONDS=10
APP_TIMEZONE=Asia/Bangkok
WARNING_BEFORE_SECONDS=300

# Sheets / Cache
SHEETS_EXECUTE_TIMEOUT_SEC=20
SHEETS_MAX_ATTEMPTS=3
SHEETS_BACKOFF_SECONDS=2
EMP_CACHE_TTL_SEC=30

# Roles
ROLES_SHEET_NAME=Roles
ROLES_CACHE_TTL_SEC=60

# Locations / Submissions
LOCATIONS_SHEET_NAME=Locations
SITE_NO_MATCH_POLICY=nearest_or_coords
SUBMISSIONS_SHEET_NAME=Submissions

# Images
IMAGE_MAX_DIM=1600
IMAGE_JPEG_QUALITY=85
IMAGE_QUALITY_CHECKIN=80
IMAGE_QUALITY_SUBMISSION=85

# Thread pool
THREAD_POOL_WORKERS=8
```

**คำแนะนำการตั้งค่า**
- เริ่มที่ค่าแนะนำข้างต้น แล้วค่อยปรับตามโหลดจริง (จำนวนผู้ใช้, ความเร็วเน็ต, โควตา Google API)  
- ถ้า Quick Reply ตำแหน่งงานเปลี่ยนไม่ทันใจ ให้ลด `ROLES_CACHE_TTL_SEC`  
- ถ้าเครือข่ายล่ม ๆ ดับ ๆ ให้เพิ่ม `SHEETS_MAX_ATTEMPTS` และ `SHEETS_BACKOFF_SECONDS`  
- หลีกเลี่ยงการตั้ง `THREAD_POOL_WORKERS` สูงเกินเหตุ อาจชนโควตา API ได้เร็ว

## 8) ความปลอดภัย/ความเป็นส่วนตัว
- ตรวจลายเซ็น `X-Line-Signature`.
- จำกัดขอบเขต OAuth (Drive) และ Service Account (Sheets).
- ไม่เก็บพิกัดดิบถ้าไม่ต้องใช้ (เก็บเฉพาะชื่อไซต์และระยะเป็นเมตร).

## 9) เกณฑ์การยอมรับ (Acceptance Criteria)
1. ลงทะเบียน: พิมพ์ชื่อ “สมชาย ใจดี” → เห็น Quick Reply ตำแหน่งจากชีต Roles → เลือกแล้วจบด้วยสรุปสองบรรทัด (ไม่มีปุ่มให้เช็คอิน/ส่งงาน).
2. เช็คอิน: เริ่ม → ส่ง location ภายใน acc≤MAX และ ts สด → ส่งรูปครบ 3 รูป → หากไม่พิมพ์ “จบ” จะไม่ปิดเอง → หมดเวลาแล้ว status=timeout และมีข้อความแจ้ง.
3. ส่งงาน: เริ่ม → ส่ง location → อัปโหลดรูป 3 รูป → ระบบปิด `done` อัตโนมัติและแจ้งสำเร็จ.
4. ตรวจรูปซ้ำ (ส่งงาน): เมื่อส่งรูปที่เคยส่งในงานอื่น ระบบบันทึกอ้างอิง P..R แต่ **ไม่** มีข้อความบอกแถว/คอลัมน์ใน LINE.
5. Roles Quick Reply: เปลี่ยนข้อมูลในชีต Roles แล้ว รายการปุ่มเปลี่ยนตาม (ภายในช่วง TTL แคช).
6. สถานการณ์เครือข่ายช้า: หากอ่านชีตล้มเหลวชั่วคราว บอทตอบด้วยข้อความ “ระบบกำลังเชื่อมต่อ Google Sheets ช้ากว่าปกติ …”.

## 10) รายงาน/การใช้งานข้อมูล (Operations)
- หัวหน้างานตรวจงานจากชีต **CheckIns** / **Submissions** (รูปอยู่บน Drive ตามลิงก์).
- คิวรี pivot จากคอลัมน์ `status`, `site_group`, `employee_name`, `created_at`.