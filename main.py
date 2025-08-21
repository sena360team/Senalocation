# main2.py
# This file will contain the main logic for the LINE Bot and backend.

from flask import Flask, request, abort
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload # Import MediaIoBaseUpload
import traceback 
import sys # Import sys module
import math # For Haversine distance calculation
from datetime import datetime # For timestamp
import uuid # For unique IDs
import io # Import io module
# import imghdr # REMOVED imghdr
from PIL import Image # Import Pillow for image type detection

# Use v3 models for compatibility with newer SDK versions
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage as V3TextMessage,
    MessagingApiBlob # Import MessagingApiBlob
) 
from linebot.v3.webhooks import MessageEvent, TextMessageContent, ImageMessageContent, LocationMessageContent
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError # Keep this for webhook handling

# Load environment variables from .env file
load_dotenv()

# --- Flask App Initialization ---
app = Flask(__name__) 

# --- Google API Configuration ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SERVICE_ACCOUNT_FILE = 'credentials.json' # Make sure this file is in the same directory as main.py

# --- Google Sheets Configuration ---
SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID') # You need to set this in your .env file

# --- Google Drive Configuration ---
GOOGLE_DRIVE_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID') # ADD THIS LINE

# Global variables for Google services
sheets_service = None
drive_service = None

def get_google_service():
    """Authenticates with Google using a service account and returns Sheets and Drive service objects."""
    global sheets_service, drive_service # Declare global to assign
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        sheets_service = build('sheets', 'v4', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        print("Google Sheets and Drive services initialized successfully.")
        sys.stdout.flush() # Flush output
        return sheets_service, drive_service
    except Exception as e:
        print(f"Error initializing Google services: {e}")
        traceback.print_exc() # Added traceback
        sys.stdout.flush() # Flush output
        return None, None

def ensure_google_services(): # NEW FUNCTION
    """Ensures Google Sheets and Drive services are initialized."""
    global sheets_service, drive_service
    if sheets_service is None or drive_service is None:
        print("DEBUG: Google services not initialized, attempting to initialize...")
        sys.stdout.flush()
        sheets_service, drive_service = get_google_service()
        if sheets_service is None or drive_service is None:
            print("ERROR: Failed to initialize Google services. Aborting.")
            sys.stdout.flush()
            abort(500)

# --- LINE Bot Configuration ---
LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET')

if not LINE_CHANNEL_ACCESS_TOKEN:
    raise ValueError("LINE_CHANNEL_ACCESS_TOKEN not set in .env")
if not LINE_CHANNEL_SECRET:
    raise ValueError("LINE_CHANNEL_SECRET not set in .env")
if not GOOGLE_DRIVE_FOLDER_ID: # ADD THIS CHECK
    raise ValueError("GOOGLE_DRIVE_FOLDER_ID not set in .env")

# Initialize LINE Messaging API client (v3)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
line_bot_api = MessagingApi(ApiClient(configuration))
handler = WebhookHandler(LINE_CHANNEL_SECRET)
blob_api = MessagingApiBlob(ApiClient(configuration)) # Initialize MessagingApiBlob

print("LINE Bot API and Webhook Handler initialized.")
sys.stdout.flush() # Flush output

# --- Google Sheets Helper Functions ---
def get_sheet_data(sheet_name):
    """Reads all data from a specified sheet."""
    print(f"DEBUG: Attempting to read from sheet: {sheet_name}")
    sys.stdout.flush()
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name).execute()
        data = result.get('values', [])
        print(f"DEBUG: Successfully read {len(data)} rows from {sheet_name}.")
        sys.stdout.flush()
        return data
    except Exception as e:
        print(f"ERROR: Error reading from sheet {sheet_name}: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        return []

def append_sheet_data(sheet_name, values):
    """Appends a row of data to a specified sheet."""
    print(f"DEBUG: Attempting to append to sheet: {sheet_name} with values: {values}")
    sys.stdout.flush()
    try:
        body = {'values': [values]}
        result = sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name,
            valueInputOption='RAW', body=body).execute()
        print(f"DEBUG: Successfully appended to {sheet_name}.")
        sys.stdout.flush()
        return result
    except Exception as e:
        print(f"ERROR: Error appending to sheet {sheet_name}: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        return None

def update_sheet_data(sheet_name, range_name, values):
    """Updates data in a specified range of a sheet."""
    print(f"DEBUG: Attempting to update sheet: {sheet_name} range: {range_name} with values: {values}")
    sys.stdout.flush()
    try:
        body = {'values': [values]}
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range_name,
            valueInputOption='RAW', body=body).execute()
        print(f"DEBUG: Successfully updated {sheet_name} at {range_name}.")
        sys.stdout.flush()
        return result
    except Exception as e:
        print(f"ERROR: Error updating sheet {sheet_name} at {range_name}: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        return None

# --- Employee State Management ---
# Column indices for Employees sheet (0-indexed)
EMPLOYEE_LINE_ID_COL = 0
EMPLOYEE_NAME_COL = 1
EMPLOYEE_POSITION_COL = 2
EMPLOYEE_CURRENT_STATE_COL = 3
EMPLOYEE_CURRENT_TRANSACTION_ID_COL = 4

def get_employee_data(user_id):
    """Retrieves a specific employee's data row from the Employees sheet."""
    employees_data = get_sheet_data("Employees")
    for i, row in enumerate(employees_data):
        if row and len(row) > EMPLOYEE_LINE_ID_COL and row[EMPLOYEE_LINE_ID_COL] == user_id:
            # Ensure the row has enough columns for state and transaction ID
            while len(row) <= EMPLOYEE_CURRENT_TRANSACTION_ID_COL:
                row.append("") # Pad with empty strings if columns are missing
            return row, i + 1 # Return row data and 1-indexed row number
    return None, None

def update_employee_state(user_id, state, transaction_id=None):
    """Updates the current_state and current_transaction_id for an employee."""
    employee_row, row_num = get_employee_data(user_id)
    if employee_row:
        employee_row[EMPLOYEE_CURRENT_STATE_COL] = state
        employee_row[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] = transaction_id if transaction_id is not None else ""
        
        # Update the specific row in Google Sheets
        # Assuming headers are in row 1, data starts from row 2
        range_name = f"Employees!A{row_num}:E{row_num}" # Adjust range based on actual columns
        return update_sheet_data("Employees", range_name, employee_row)
    return None

# --- Location Calculation Helper ---
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000  # Radius of Earth in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c  # Distance in meters
    return distance

# --- Webhook Endpoint ---
@app.route("/callback", methods=['POST'])
def callback():
    # Ensure Google services are initialized
    ensure_google_services()

    print("Webhook callback received!") # Add this line for debugging
    sys.stdout.flush() # Flush output
    signature = request.headers.get('X-Line-Signature') # Use .get() for safety
    if not signature:
        print("ERROR: Missing X-Line-Signature header.")
        sys.stdout.flush()
        abort(400, description="Missing X-Line-Signature")

    body = request.get_data(as_text=True)
    print("Request body: " + body)
    print("X-Line-Signature: " + signature)
    sys.stdout.flush() # Flush output

    try:
        handler.handle(body, signature)
    except InvalidSignatureError as e:
        print("ERROR: Invalid LINE signature. Check LINE_CHANNEL_SECRET.")
        print(f"Exception details: {e}")
        traceback.print_exc() # Print full traceback for InvalidSignatureError
        sys.stdout.flush() # Flush output
        abort(400)
    except Exception as e:
        print(f"ERROR: Error handling webhook: {e}")
        print(f"Exception details: {e}")
        traceback.print_exc() # Print full traceback for other exceptions
        sys.stdout.flush() # Flush output
        abort(500) # Internal Server Error for unhandled exceptions

    return 'OK'

# --- LIFF App Serving Route ---
# @app.route('/liff_location_picker') # Removed LIFF route
# def serve_liff_location_picker():
#     print("DEBUG: Serving liff_location_picker.html")
#     sys.stdout.flush()
#     try:
#         with open('liff_location_picker.html', 'r', encoding='utf-8') as f:
#             html_content = f.read()
#         html_content = html_content.replace("YOUR_LIFF_ID", LIFF_ID)
#         print("DEBUG: liff_location_picker.html served successfully.")
#         sys.stdout.flush()
#         return html_content
#     except FileNotFoundError:
#         print("ERROR: liff_location_picker.html not found.")
#         traceback.print_exc()
#         sys.stdout.flush()
#         abort(404)
#     except Exception as e:
#         print(f"ERROR: Error serving liff_location_picker.html: {e}")
#         traceback.print_exc()
#         sys.stdout.flush()
#         abort(500)

# --- Message Handler ---
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    # Ensure Google services are initialized
    ensure_google_services()

    user_id = event.source.user_id
    text = event.message.text.strip()

    print(f"DEBUG: User ID: {user_id}")
    sys.stdout.flush()

    # Get employee data and state
    employee_data, _ = get_employee_data(user_id)
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if employee_data else "idle"
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if employee_data else ""

    print(f"DEBUG: User {user_id} current_state: {current_state}, current_transaction_id: {current_transaction_id}")
    sys.stdout.flush()

    # FR-001: User Registration
    if text == "ลงทะเบียน":
        print("DEBUG: Received 'ลงทะเบียน' command.")
        sys.stdout.flush()
        
        if employee_data: # User already exists
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="คุณลงทะเบียนแล้ว")]
                )
            )
            print("DEBUG: Replied: คุณลงทะเบียนแล้ว")
            sys.stdout.flush()
        else:
            # Set state to waiting for registration info
            # For simplicity, we assume the next message is the full registration info
            # A more robust solution would set a state like 'waiting_for_registration_input'
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="กรุณาพิมพ์ชื่อ-นามสกุลและตำแหน่งงานของคุณ คั่นด้วยคอมม่า (,)
เช่น สมชาย ใจดี, ผู้จัดการ")]
                )
            )
            print("DEBUG: Replied: กรุณาพิมพ์ชื่อ-นามสกุลและตำแหน่งงานของคุณ...")
            sys.stdout.flush()
    # FR-002: Check-in (Initiate native LocationMessage)
    elif text == "เช็คอิน":
        print("DEBUG: Received 'เช็คอิน' command.")
        sys.stdout.flush()
        
        if not employee_data: # User not registered
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันเช็คอิน พิมพ์ \"ลงทะเบียน\"")]
                )
            )
            print("DEBUG: Replied: กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันเช็คอิน...")
            sys.stdout.flush()
            return

        # Ask user to send location via LINE's native location picker
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    V3TextMessage(text="กรุณาส่งตำแหน่งปัจจุบันของคุณเพื่อเช็คอิน")
                ]
            )
        )
        print("DEBUG: Replied: กรุณาส่งตำแหน่งปัจจุบันของคุณ...")
        sys.stdout.flush()

    elif "," in text: # This is a very weak check for registration data, assumes user is trying to register
        print("DEBUG: Received comma-separated text.")
        sys.stdout.flush()
        
        if not employee_data: # User not registered, process as registration input
            parts = [p.strip() for p in text.split(',', 1)] # Split only once
            if len(parts) == 2:
                user_name = parts[0]
                user_position = parts[1]
                
                append_sheet_data("Employees", [user_id, user_name, user_position, "idle", ""]) # Initial state after registration
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="ลงทะเบียนสำเร็จแล้ว")]
                    )
                )
                print("DEBUG: Replied: ลงทะเบียนสำเร็จแล้ว")
                sys.stdout.flush()
            else:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="รูปแบบข้อมูลไม่ถูกต้อง กรุณาพิมพ์ชื่อ-นามสกุลและตำแหน่งงานของคุณ คั่นด้วยคอมม่า (,)
เช่น สมชาย ใจดี, ผู้จัดการ")]
                    )
                )
                print("DEBUG: Replied: รูปแบบข้อมูลไม่ถูกต้อง...")
                sys.stdout.flush()
        else: # User is already registered, but sent comma-separated text (unhandled state)
            print("DEBUG: User already registered, but sent comma-separated text. Echoing.")
            sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text=event.message.text)]
                )
            )
            print("DEBUG: Replied: Echoing text.")
            sys.stdout.flush()
    
    # --- Handle states for image uploads (FR-003) ---
    elif current_state == "waiting_for_checkin_images":
        print("DEBUG: User is in waiting_for_checkin_images state.")
        sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาส่งรูปภาพหลักฐาน (สูงสุด 3 รูป)")]
            )
        )
        print("DEBUG: Replied: กรุณาส่งรูปภาพหลักฐาน...")
        sys.stdout.flush()

    else:
        # Default echo for other messages
        print(f"DEBUG: Received unhandled text: {text}")
        sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text=event.message.text)]
            )
        )
        print("DEBUG: Replied: Echoing text.")
        sys.stdout.flush()

@handler.add(MessageEvent, message=LocationMessageContent)
def handle_location_message(event):
    # Ensure Google services are initialized
    ensure_google_services()

    user_id = event.source.user_id
    user_lat = event.message.latitude
    user_lon = event.message.longitude

    print(f"DEBUG: Received location from {user_id}: Lat={user_lat}, Lon={user_lon}")
    sys.stdout.flush()

    # Get employee data and state
    employee_data, row_num = get_employee_data(user_id)
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if employee_data else "idle"
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if employee_data else ""

    print(f"DEBUG: User {user_id} current_state: {current_state}, current_transaction_id: {current_transaction_id}")
    sys.stdout.flush()

    if not employee_data: # User not registered
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันเช็คอิน พิมพ์ \"ลงทะเบียน\"")]
            )
        )
        print("DEBUG: Replied: กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันเช็คอิน (Location)...")
        sys.stdout.flush()
        return

    # Get locations data from Google Sheet
    locations_data = get_sheet_data("Locations")
    print(f"DEBUG: Locations data from sheet: {locations_data}")
    sys.stdout.flush()
    if not locations_data or len(locations_data) <= 1: # Check if sheet is empty or only has headers
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ไม่พบข้อมูลสถานที่สำหรับเช็คอิน กรุณาติดต่อผู้ดูแลระบบ")]
            )
        )
        print("DEBUG: Replied: ไม่พบข้อมูลสถานที่สำหรับเช็คอิน...")
        sys.stdout.flush()
        return

    # Skip header row
    locations_data_without_header = locations_data[1:] 

    closest_location = None
    min_distance = float('inf')
    location_name = ""
    site_group = ""

    for row in locations_data_without_header:
        try:
            # Ensure row has enough columns and data is convertible
            if len(row) >= 5:
                loc_name = row[0]
                loc_site_group = row[1]
                loc_lat = float(row[2])
                loc_lon = float(row[3])
                checkin_radius = float(row[4])

                distance = haversine_distance(user_lat, user_lon, loc_lat, loc_lon)
                print(f"DEBUG: Distance to {loc_name} ({loc_site_group}): {distance:.2f} meters (Radius: {checkin_radius}m)")
                sys.stdout.flush()

                if distance <= checkin_radius:
                    if distance < min_distance:
                        min_distance = distance
                        closest_location = row
                        location_name = loc_name
                        site_group = loc_site_group
        except ValueError as ve:
            print(f"ERROR: Invalid data in Locations sheet row: {row}. Error: {ve}")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR: Error processing location row: {row}. Error: {e}")
            sys.stdout.flush()

    if closest_location:
        # Record check-in
        checkin_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        append_sheet_data("CheckIns", 
                          [checkin_id, timestamp, user_id, location_name, site_group, "", "", ""]) # Image URLs will be added later
        
        # Update employee state to waiting for images
        update_employee_state(user_id, "waiting_for_checkin_images", checkin_id)

        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text=f"เช็คอินสำเร็จที่ {location_name} ({site_group}) ระยะห่าง {min_distance:.2f} เมตร\nกรุณาส่งรูปภาพหลักฐาน (สูงสุด 3 รูป)")]
            )
        )
        print(f"DEBUG: Replied: เช็คอินสำเร็จที่ {location_name}... และขอรูปภาพ")
        sys.stdout.flush()
    else:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="คุณอยู่นอกพื้นที่เช็คอินที่กำหนด")]
            )
        )
        print("DEBUG: Replied: คุณอยู่นอกพื้นที่เช็คอิน...")
        sys.stdout.flush()

# --- Image Message Handler (FR-003) ---
@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message(event):
    # Ensure Google services are initialized
    ensure_google_services()

    user_id = event.source.user_id
    message_id = event.message.id

    print(f"DEBUG: Received image from {user_id}, message_id: {message_id}")
    sys.stdout.flush()

    # Get employee data and state
    employee_data, row_num = get_employee_data(user_id)
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if employee_data else "idle"
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if employee_data else ""

    print(f"DEBUG: User {user_id} current_state: {current_state}, current_transaction_id: {current_transaction_id}")
    sys.stdout.flush()

    if not employee_data: # User not registered
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันนี้")]
            )
        )
        print("DEBUG: Replied: กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันนี้ (Image)...")
        sys.stdout.flush()
        return

    if current_state == "waiting_for_checkin_images" and current_transaction_id:
        try:
            # Get image content from LINE
            image_bytes = blob_api.get_message_content(message_id)

            # Detect image type and MIME
            kind = imghdr.what(None, h=image_bytes)  # 'jpeg' | 'png' | 'gif' | None
            mime = {'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif'}.get(kind, 'application/octet-stream')
            ext  = 'jpg' if kind == 'jpeg' else (kind or 'bin')
            file_name = f"checkin_image_{current_transaction_id}_{uuid.uuid4()}.{ext}"

            # Upload non-resumable from memory
            bio = io.BytesIO(image_bytes)
            media = MediaIoBaseUpload(bio, mimetype=mime, resumable=False)

            file_metadata = {
                'name': file_name,
                'parents': [GOOGLE_DRIVE_FOLDER_ID] # Use GOOGLE_DRIVE_FOLDER_ID from .env
            }
            
            uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink', supportsAllDrives=True).execute()
            image_url = uploaded_file.get('webViewLink')
            print(f"DEBUG: Image uploaded to Drive: {image_url}")
            sys.stdout.flush()

            # Set public permission for the uploaded file
            drive_service.permissions().create(
                fileId=uploaded_file['id'],
                body={'type': 'anyone', 'role': 'reader'},
                supportsAllDrives=True
            ).execute()
            print("DEBUG: Image permission set to public.")
            sys.stdout.flush()

            # Update CheckIns sheet with image URL
            checkins_data = get_sheet_data("CheckIns")
            updated = False
            for i, row in enumerate(checkins_data):
                if row and len(row) > 0 and row[0] == current_transaction_id:
                    # Find the next available image_url column (image_url_1, image_url_2, image_url_3)
                    for j in range(5, 8): # Columns 5, 6, 7 (0-indexed) for image_url_1, 2, 3
                        if len(row) <= j or not row[j]: # If column doesn't exist or is empty
                            # Ensure row has enough columns
                            while len(row) <= j:
                                row.append("")
                            row[j] = image_url
                            updated = True
                            # Update the specific row in Google Sheets
                            range_name = f"CheckIns!A{i+1}:H{i+1}" # Adjust range based on actual columns
                            update_sheet_data("CheckIns", range_name, row)
                            break
                    if updated:
                        break
            
            # Count images for this transaction to decide state update
            images_now = 0
            # Re-read the updated row to be sure (or pass it from previous update_sheet_data call)
            # For simplicity, re-reading here. In a real app, optimize this.
            checkins_data_after_update = get_sheet_data("CheckIns")
            for row_after_update in checkins_data_after_update:
                if row_after_update and row_after_update[0] == current_transaction_id:
                    for j in range(5, 8):  # F..H = image_url_1..3
                        if j < len(row_after_update) and row_after_update[j]:
                            images_now += 1
                    break

            if images_now >= 3: # If 3 or more images are uploaded, reset state
                update_employee_state(user_id, "idle", "")
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปภาพหลักฐานครบถ้วนแล้ว\nคุณสามารถดำเนินการอื่นต่อได้")]
                    )
                )
                print("DEBUG: Replied: บันทึกรูปภาพหลักฐานครบถ้วนแล้ว...")
                sys.stdout.flush()
            else:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปภาพหลักฐานเรียบร้อยแล้ว\nคุณสามารถส่งรูปภาพเพิ่มเติมได้อีก (สูงสุด 3 รูป) หรือส่งข้อความอื่นเพื่อดำเนินการต่อ")]
                )
            )
            print("DEBUG: Replied: บันทึกรูปภาพหลักฐานเรียบร้อยแล้ว...")
            sys.stdout.flush()

        except Exception as e:
            print(f"ERROR: Error handling image message: {e}")
            traceback.print_exc()
            sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="เกิดข้อผิดพลาดในการบันทึกรูปภาพ กรุณาลองใหม่อีกครั้ง")]
                )
            )
            print("DEBUG: Replied: เกิดข้อผิดพลาดในการบันทึกรูปภาพ...")
            sys.stdout.flush()

    else:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาเช็คอินก่อนส่งรูปภาพ")]
            )
        )
        print("DEBUG: Replied: กรุณาเช็คอินก่อนส่งรูปภาพ...")
        sys.stdout.flush()

# --- Main execution ---
if __name__ == '__main__':
    sheets_service, drive_service = get_google_service()
    if sheets_service and drive_service:
        print("Ready to interact with Google Sheets and Drive.")
        sys.stdout.flush() # Flush output
    else:
        print("Failed to initialize Google services.")
        sys.stdout.flush() # Flush output

    # --- IMPORTANT: Google Sheet Setup Reminder ---
    print("\n--- Google Sheet Setup Reminder ---")
    print("Please ensure you have the following sheets in your Google Spreadsheet:")
    print("1. Employees (Headers: line_id, employee_name, position, current_state, current_transaction_id)")
    print("2. Locations (Headers: location_name, site_group, latitude, longitude, checkin_radius_meters, submission_radius_meters)")
    print("3. CheckIns (Headers: checkin_id, timestamp, line_id, location_name, site_group, image_url_1, image_url_2, image_url_3)")
    print("-----------------------------------\n")
    sys.stdout.flush()

    app.run(host='0.0.0.0', port=8000)
# Last updated: 2025-08-13 17:15:00
