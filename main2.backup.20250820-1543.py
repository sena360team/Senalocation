# main2.py
# This file will contain the main logic for the LINE Bot and backend.

from flask import Flask, request, abort, redirect, url_for, session, Response, render_template_string
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload # Import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
import traceback 
import sys # Import sys module
import math # For Haversine distance calculation
from datetime import datetime # For timestamp
import uuid # For unique IDs
import io # Import io module
# import imghdr # REMOVED imghdr
from PIL import Image # Import Pillow for image type detection
import threading  # For simple in-process locking
from collections import defaultdict  # For lock registry
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
import re  # for tolerant text matching

# OAuth imports
from google.oauth2.credentials import Credentials # Added
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
import pickle # For storing token.pickle
import os.path # For checking token.pickle file
import json # For saving token.json
import pathlib # For checking token.json path

# Use v3 models for compatibility with newer SDK versions
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, ReplyMessageRequest,
    TextMessage as V3TextMessage,
    MessagingApiBlob,  # Import MessagingApiBlob
    PushMessageRequest  # For background notifications
) 

# --- Quick Reply model imports for Quick Reply ---
from linebot.v3.messaging.models import (
    QuickReply,
    QuickReplyItem,
    URIAction,
    MessageAction,
    CameraAction,
    CameraRollAction,
)
from apscheduler.schedulers.background import BackgroundScheduler
from linebot.v3.webhooks import MessageEvent, TextMessageContent, ImageMessageContent, LocationMessageContent
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError # Keep this for webhook handling

# Load environment variables from .env file
load_dotenv()
print(f"DEBUG: GOOGLE_SHEET_ID from os.getenv: {os.getenv('GOOGLE_SHEET_ID')}")
print(f"DEBUG: GOOGLE_DRIVE_FOLDER_ID from os.getenv: {os.getenv('GOOGLE_DRIVE_FOLDER_ID')}")
print(f"DEBUG: FLASK_SECRET_KEY from os.getenv: {os.getenv('FLASK_SECRET_KEY')}")
print(f"DEBUG: GOOGLE_CLIENT_ID from os.getenv: {os.getenv('GOOGLE_CLIENT_ID')}")
print(f"DEBUG: GOOGLE_CLIENT_SECRET from os.getenv: {os.getenv('GOOGLE_CLIENT_SECRET')}")
print(f"DEBUG: GOOGLE_REDIRECT_URI from os.getenv: {os.getenv('GOOGLE_REDIRECT_URI')}")
sys.stdout.flush()

# --- LIFF / Anti-fraud Configuration ---
LIFF_ID = os.getenv('LIFF_ID', '').strip()
MAX_GPS_ACCURACY_M = int(os.getenv('MAX_GPS_ACCURACY_M', '50'))       # accept accuracy <= 50m
MAX_LOCATION_AGE_SEC = int(os.getenv('MAX_LOCATION_AGE_SEC', '60'))   # client ts age <= 60 sec
print(f"DEBUG: LIFF_ID = {'set' if LIFF_ID else 'NOT SET'}")
print(f"DEBUG: MAX_GPS_ACCURACY_M = {MAX_GPS_ACCURACY_M}")
print(f"DEBUG: MAX_LOCATION_AGE_SEC = {MAX_LOCATION_AGE_SEC}")
sys.stdout.flush()

# --- Flask App Initialization ---
app = Flask(__name__) 
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me") # Required for Flask sessions

# Apply ProxyFix for ngrok/proxy support
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config['PREFERRED_URL_SCHEME'] = 'https'

# --- Google API Configuration ---
# OAuth Scopes for Drive access
OAUTH_SCOPES = [
    'https://www.googleapis.com/auth/drive.file', # Access to files created or opened by the app
    'https://www.googleapis.com/auth/drive.appdata', # Access to the application data folder
    'https://www.googleapis.com/auth/drive.metadata.readonly' # Read-only access to file metadata
]

# Service Account Scopes (for Sheets, if still used)
SERVICE_ACCOUNT_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets'
]
SERVICE_ACCOUNT_FILE = 'credentials.json' # Make sure this file is in the same directory as main.py


# --- Google Sheets Configuration ---
SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID') # You need to set this in your .env file
print(f"DEBUG: SPREADSHEET_ID assigned: {SPREADSHEET_ID}") # New print
sys.stdout.flush()

# --- Google Drive Target Folder (OAuth) ---
GOOGLE_DRIVE_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
print(f"DEBUG: GOOGLE_DRIVE_FOLDER_ID assigned: {GOOGLE_DRIVE_FOLDER_ID}")
if not GOOGLE_DRIVE_FOLDER_ID:
    print("WARNING: GOOGLE_DRIVE_FOLDER_ID not set in .env; Drive uploads will fail.")
sys.stdout.flush()

# --- Google Drive Configuration (OAuth) ---
# These will be used for OAuth flow
GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REDIRECT_URI = os.getenv('GOOGLE_REDIRECT_URI')

# --- Check-in Timeout (seconds) ---
CHECKIN_TIMEOUT_SECONDS = int(os.getenv('CHECKIN_TIMEOUT_SECONDS', '180'))
print(f"DEBUG: CHECKIN_TIMEOUT_SECONDS = {CHECKIN_TIMEOUT_SECONDS}")
sys.stdout.flush()

# --- Scheduler Configuration ---
SCHEDULER_INTERVAL_SECONDS = int(os.getenv('SCHEDULER_INTERVAL_SECONDS', '30'))
APP_TIMEZONE = os.getenv('APP_TIMEZONE', 'Asia/Bangkok')
WARNING_BEFORE_SECONDS = int(os.getenv('WARNING_BEFORE_SECONDS', '10'))
print(f"DEBUG: SCHEDULER_INTERVAL_SECONDS = {SCHEDULER_INTERVAL_SECONDS}")
print(f"DEBUG: APP_TIMEZONE = {APP_TIMEZONE}")
print(f"DEBUG: WARNING_BEFORE_SECONDS = {WARNING_BEFORE_SECONDS}")
sys.stdout.flush()


# --- Google API call timeouts (seconds) ---
SHEETS_EXECUTE_TIMEOUT_SEC = int(os.getenv('SHEETS_EXECUTE_TIMEOUT_SEC', '20'))  # hard cap per API call
DRIVE_EXECUTE_TIMEOUT_SEC  = int(os.getenv('DRIVE_EXECUTE_TIMEOUT_SEC',  '15'))
print(f"DEBUG: SHEETS_EXECUTE_TIMEOUT_SEC = {SHEETS_EXECUTE_TIMEOUT_SEC}")
print(f"DEBUG: DRIVE_EXECUTE_TIMEOUT_SEC  = {DRIVE_EXECUTE_TIMEOUT_SEC}")
sys.stdout.flush()

# --- Image compression settings ---
IMAGE_MAX_DIM = int(os.getenv('IMAGE_MAX_DIM', '1600'))       # max width/height in px
# Backward-compatible default; still used if specific per-flow quality not provided
IMAGE_JPEG_QUALITY = int(os.getenv('IMAGE_JPEG_QUALITY', '80'))  # JPEG quality (legacy/default)
# New per-flow quality controls (override legacy if set)
IMAGE_QUALITY_CHECKIN = int(os.getenv('IMAGE_QUALITY_CHECKIN', '75'))
IMAGE_QUALITY_SUBMISSION = int(os.getenv('IMAGE_QUALITY_SUBMISSION', '90'))

# --- Robust retry/backoff config for Google Sheets ---
SHEETS_MAX_ATTEMPTS = int(os.getenv('SHEETS_MAX_ATTEMPTS', '3'))
SHEETS_BACKOFF_SECONDS = float(os.getenv('SHEETS_BACKOFF_SECONDS', '1.5'))
print(f"DEBUG: SHEETS_MAX_ATTEMPTS = {SHEETS_MAX_ATTEMPTS}")
print(f"DEBUG: SHEETS_BACKOFF_SECONDS = {SHEETS_BACKOFF_SECONDS}")
sys.stdout.flush()

def _sheets_exec_with_retry(request_callable, desc: str):
    """
    Execute a Google Sheets request with hard timeout AND exponential backoff.
    Returns the JSON dict on success, or raises the last Exception on failure.
    """
    attempt = 0
    delay = SHEETS_BACKOFF_SECONDS
    last_exc = None
    while attempt < SHEETS_MAX_ATTEMPTS:
        attempt += 1
        try:
            return _exec_with_timeout(lambda: request_callable().execute(num_retries=3),
                                      SHEETS_EXECUTE_TIMEOUT_SEC,
                                      f"{desc} (attempt {attempt}/{SHEETS_MAX_ATTEMPTS})")
        except Exception as e:
            last_exc = e
            print(f"WARNING: {desc} failed on attempt {attempt}/{SHEETS_MAX_ATTEMPTS}: {e}")
            traceback.print_exc()
            sys.stdout.flush()
            if attempt >= SHEETS_MAX_ATTEMPTS:
                break
            time.sleep(delay)
            delay *= 2  # exponential backoff
    # Exhausted attempts
    raise last_exc

# --- Locations matching configuration ---
LOCATIONS_SHEET_NAME = os.getenv('LOCATIONS_SHEET_NAME', 'Locations')
SITE_NO_MATCH_POLICY = os.getenv('SITE_NO_MATCH_POLICY', 'nearest_or_coords')  # 'nearest_or_coords' | 'coords_only' | 'reject'
print(f"DEBUG: LOCATIONS_SHEET_NAME = {LOCATIONS_SHEET_NAME}")
print(f"DEBUG: SITE_NO_MATCH_POLICY = {SITE_NO_MATCH_POLICY}")
sys.stdout.flush()

# --- Submissions sheet configuration ---
SUBMISSIONS_SHEET_NAME = os.getenv('SUBMISSIONS_SHEET_NAME', 'Submissions')

# Validate GOOGLE_REDIRECT_URI for HTTPS
if GOOGLE_REDIRECT_URI and not GOOGLE_REDIRECT_URI.startswith("https://"):
    raise ValueError("GOOGLE_REDIRECT_URI must use HTTPS (e.g., https://your-ngrok-url/oauth2callback)")

TOKEN_PATH = "token.json" # Path to store OAuth token
CLIENT_SECRETS_FILE = "client_secret.json" # Path to client_secret.json downloaded from Cloud Console
print(f"DEBUG: CLIENT_SECRETS_FILE path: {os.path.abspath(CLIENT_SECRETS_FILE)}") # New print
sys.stdout.flush()

 # Global variables for Google services
sheets_service = None
drive_service = None
# Background scheduler (initialized in __main__)
scheduler = None

# ---------- Simple cache for Employees sheet ----------
_EMP_CACHE = {"rows": None, "ts": 0.0}
EMP_CACHE_TTL_SEC = float(os.getenv("EMP_CACHE_TTL_SEC", "30"))
# -----------------------------------------------------

# In-memory locks per transaction to avoid race conditions when multiple images arrive nearly simultaneously
_txn_locks = defaultdict(threading.Lock)

# Cache: checkin_id -> row index (1-based) to allow partial updates when Sheets read times out
_checkins_row_index_cache = {}

def get_drive_service_oauth():
    """Authenticates with Google using OAuth 2.0 and returns Drive service object."""
    creds = None
    if pathlib.Path(TOKEN_PATH).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, OAUTH_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # No valid creds, user needs to authorize via /authorize route
            print("DEBUG: No valid OAuth credentials found. User needs to authorize.")
            sys.stdout.flush()
            return None
    
    # Save the credentials for the next run
    with open(TOKEN_PATH, 'w') as f:
        f.write(creds.to_json())

    # Build with credentials only (avoid passing http together with credentials)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def get_google_service_sheets():
    """Authenticates with Google using a service account for Sheets access (with HTTP timeout)."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SERVICE_ACCOUNT_SCOPES)
        # Build with credentials only (newer googleapiclient disallows http+credentials together)
        return build('sheets', 'v4', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"Error initializing Google Sheets service account: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        return None

def ensure_google_services(): # Modified to handle both OAuth and Service Account
    """Ensures Google Sheets and Drive services are initialized."""
    global sheets_service, drive_service
    if sheets_service is None:
        sheets_service = get_google_service_sheets()
        if sheets_service is None:
            print("ERROR: Failed to initialize Google Sheets service. Aborting.")
            sys.stdout.flush()
            abort(500)
    
    if drive_service is None:
        drive_service = get_drive_service_oauth()
        if drive_service is None:
            print("ERROR: Google Drive service not authorized. Please authorize via /authorize.")
            sys.stdout.flush()
            # We don't abort here, but expect the bot to handle cases where Drive is not ready.

# --- LINE Bot Configuration ---
LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET')

if not LINE_CHANNEL_ACCESS_TOKEN:
    raise ValueError("LINE_CHANNEL_ACCESS_TOKEN not set in .env")
if not LINE_CHANNEL_SECRET:
    raise ValueError("LINE_CHANNEL_SECRET not set in .env")

# OAuth Client ID/Secret/Redirect URI checks
if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
    print("WARNING: Google OAuth Client ID, Secret, or Redirect URI not fully set in .env. OAuth flow may fail.")
    sys.stdout.flush()

# Initialize LINE Messaging API client (v3)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
line_bot_api = MessagingApi(ApiClient(configuration))
handler = WebhookHandler(LINE_CHANNEL_SECRET)
blob_api = MessagingApiBlob(ApiClient(configuration)) # Initialize MessagingApiBlob

print("LINE Bot API and Webhook Handler initialized.")
sys.stdout.flush() # Flush output

# --- Blocking-call timeout wrapper ---
THREAD_POOL_WORKERS = int(os.getenv('THREAD_POOL_WORKERS', '8'))
print(f"DEBUG: THREAD_POOL_WORKERS = {THREAD_POOL_WORKERS}")
sys.stdout.flush()
_executor_singleton = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)

def _exec_with_timeout(fn, timeout_sec, desc=''):
    """Run a callable in a thread and enforce a hard timeout. Raise TimeoutError on expiry."""
    try:
        fut = _executor_singleton.submit(fn)
        return fut.result(timeout=timeout_sec)
    except FuturesTimeout:
        raise TimeoutError(f"Timeout while executing {desc or 'Google API call'} after {timeout_sec}s")

# --- Google Sheets Helper Functions ---
def _col_letter(n: int) -> str:
    """1-based column index -> ตัวอักษรคอลัมน์แบบ Excel (1=A, 13=M, 27=AA, ...)"""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _update_row_dynamic(sheet_name: str, row_idx_1based: int, row_values: list):
    """
    อัปเดตทั้งแถวโดยให้ช่วงคอลัมน์สิ้นสุดตามความยาวของ row_values อัตโนมัติ
    ป้องกันเคสกำหนดช่วงแค่ A..L แต่ส่งค่าไปถึงคอลัมน์ M แล้วโดน 400
    """
    end_col_letter = _col_letter(len(row_values))
    rng = f"{sheet_name}!A{row_idx_1based}:{end_col_letter}{row_idx_1based}"
    return update_sheet_data(sheet_name, rng, row_values)

def get_sheet_data(sheet_name):
    """Reads all data from a specified sheet (with cache for Employees)."""
    # Cache only for Employees to reduce API pressure in hot paths/scheduler
    use_cache = (sheet_name == "Employees")
    now = time.time()

    if use_cache and _EMP_CACHE["rows"] is not None and (now - _EMP_CACHE["ts"] <= EMP_CACHE_TTL_SEC):
        print(f"DEBUG: Employees cache hit ({len(_EMP_CACHE['rows'])} rows).")
        sys.stdout.flush()
        return _EMP_CACHE["rows"]

    print(f"DEBUG: Attempting to read from sheet: {sheet_name}")
    sys.stdout.flush()
    try:
        request = lambda: sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name)
        result = _sheets_exec_with_retry(request, f"Sheets get({sheet_name})")
        data = result.get('values', [])

        if use_cache:
            _EMP_CACHE["rows"] = data
            _EMP_CACHE["ts"] = now

        print(f"DEBUG: Successfully read {len(data)} rows from {sheet_name}.")
        sys.stdout.flush()
        return data
    except Exception as e:
        print(f"ERROR: Error reading from sheet {sheet_name}: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        # If Employees read fails but we have a recent cache, serve stale data
        if use_cache and _EMP_CACHE["rows"] is not None:
            print("WARNING: Using stale Employees cache due to Sheets error.")
            sys.stdout.flush()
            return _EMP_CACHE["rows"]
        return None

# --- Quick-read helper for Sheets (no retries, hard timeout) ---
def get_sheet_data_quick(sheet_name, timeout_sec=8):
    """อ่านชีตแบบเร็ว ไม่ retry หลายรอบ เพื่อลดเวลาค้างใน scheduler."""
    print(f"DEBUG: QUICK read from sheet: {sheet_name}")
    sys.stdout.flush()
    try:
        req = lambda: sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name)
        # no extra retries inside request, just our outer hard-timeout
        res = _exec_with_timeout(lambda: req().execute(num_retries=0),
                                 timeout_sec,
                                 f"Sheets quick get({sheet_name})")
        data = res.get('values', [])
        print(f"DEBUG: QUICK read ok: {sheet_name} rows={len(data)}")
        sys.stdout.flush()
        return data
    except Exception as e:
        print(f"WARNING: QUICK read failed for {sheet_name}: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        return None

def append_sheet_data(sheet_name, values):
    """Appends a row of data to a specified sheet."""
    print(f"DEBUG: Attempting to append to sheet: {sheet_name} with values: {values}")
    sys.stdout.flush()
    try:
        body = {'values': [values]}
        request = lambda: sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name,
            valueInputOption='RAW', body=body)
        result = _sheets_exec_with_retry(request, f"Sheets append({sheet_name})")
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
        request = lambda: sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range_name,
            valueInputOption='RAW', body=body)
        result = _sheets_exec_with_retry(request, f"Sheets update({range_name})")
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
    if employees_data is None:
        # Propagate a sentinel indicating transient Sheets failure
        return ("__SHEETS_ERROR__", None)
    for i, row in enumerate(employees_data):
        if row and len(row) > EMPLOYEE_LINE_ID_COL and row[EMPLOYEE_LINE_ID_COL] == user_id:
            # Ensure the row has enough columns for state and transaction ID
            while len(row) <= EMPLOYEE_CURRENT_TRANSACTION_ID_COL:
                row.append("") # Pad with empty strings if columns are missing
            return row, i + 1 # Return row data and 1-indexed row number
    return None, None

# --- Employee name fetch helper ---
def get_employee_name(user_id: str) -> str:
    """
    Helper to fetch employee display name from Employees sheet.
    Returns empty string if unavailable or on transient Sheets error.
    """
    data, _ = get_employee_data(user_id)
    if data == "__SHEETS_ERROR__" or not data:
        return ""
    return data[EMPLOYEE_NAME_COL] if len(data) > EMPLOYEE_NAME_COL else ""

def update_employee_state(user_id, state, transaction_id=None):
    """Updates the current_state and current_transaction_id for an employee."""
    employee_row, row_num = get_employee_data(user_id)
    if employee_row == "__SHEETS_ERROR__":
        print("ERROR: update_employee_state skipped due to Sheets read failure.")
        sys.stdout.flush()
        return None
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

# --- Locations loader & matcher ---
def load_locations(sheet_name=None):
    """Load locations from Google Sheet `Locations`.
    Expected header at row1:
      location_name | site_group | latitude | longitude | checkin_radius_meters | submission_radius_meters | radius_m
    Returns list of dicts with keys: name, group, lat, lon, checkin_radius, submission_radius, radius_m
    """
    sheet = sheet_name or LOCATIONS_SHEET_NAME
    rows = get_sheet_data(sheet)
    locs = []
    if not rows or len(rows) < 2:
        return locs
    # assume first row is header
    for r in rows[1:]:
        if not r or len(r) < 5:
            continue
        try:
            name = (r[0] or "").strip()
            group = (r[1] or "").strip() if len(r) > 1 else ""
            lat = float(r[2]) if len(r) > 2 and r[2] not in (None, "") else None
            lon = float(r[3]) if len(r) > 3 and r[3] not in (None, "") else None
            checkin_radius = float(r[4]) if len(r) > 4 and r[4] not in (None, "") else 0.0
            submission_radius = float(r[5]) if len(r) > 5 and r[5] not in (None, "") else 0.0
            radius_m = float(r[6]) if len(r) > 6 and r[6] not in (None, "") else 0.0
            if name and lat is not None and lon is not None:
                locs.append({
                    "name": name,
                    "group": group,
                    "lat": lat,
                    "lon": lon,
                    "checkin_radius": checkin_radius,
                    "submission_radius": submission_radius,
                    "radius_m": radius_m,
                })
        except Exception:
            continue
    return locs

def match_site_by_location(lat, lon, policy=None):
    """Return (location_name, site_group, matched, nearest_dist_m).
    matched=True if within any location's **checkin_radius_meters**.
    If no match:
      - 'nearest_or_coords': return nearest location's name/group if exists, else coords; matched=False
      - 'coords_only': return f"{lat},{lon}" and empty group; matched=False
      - 'reject': return (None, None, False, None)
    """
    pol = (policy or SITE_NO_MATCH_POLICY).lower().strip()
    locs = load_locations()
    if not locs:
        # No locations configured
        return (f"{lat},{lon}", "", False, None)

    nearest = None
    nearest_d = None
    for s in locs:
        d = haversine_distance(lat, lon, s["lat"], s["lon"])
        # Exact match within check-in radius only
        if s.get("checkin_radius", 0) and d <= float(s.get("checkin_radius", 0)):
            return (s["name"], s["group"], True, d)
        if nearest_d is None or d < nearest_d:
            nearest = s
            nearest_d = d

    # No radius match
    if pol == "nearest_or_coords":
        if nearest:
            return (nearest["name"], nearest["group"], False, nearest_d)
        return (f"{lat},{lon}", "", False, None)
    elif pol == "coords_only":
        return (f"{lat},{lon}", "", False, nearest_d)
    elif pol == "reject":
        return (None, None, False, nearest_d)
    # default fallback
    return (f"{lat},{lon}", "", False, nearest_d)

# --- Match site for submission flow (uses submission_radius) ---
def match_site_by_location_for_submission(lat, lon, policy=None):
    pol = (policy or SITE_NO_MATCH_POLICY).lower().strip()
    locs = load_locations()
    if not locs:
        return (f"{lat},{lon}", "", False, None)
    nearest = None
    nearest_d = None
    for s in locs:
        d = haversine_distance(lat, lon, s["lat"], s["lon"])
        # Match within submission_radius
        if s.get("submission_radius", 0) and d <= float(s.get("submission_radius", 0)):
            return (s["name"], s["group"], True, d)
        if nearest_d is None or d < nearest_d:
            nearest = s
            nearest_d = d
    if pol == "nearest_or_coords":
        if nearest:
            return (nearest["name"], nearest["group"], False, nearest_d)
        return (f"{lat},{lon}", "", False, None)
    elif pol == "coords_only":
        return (f"{lat},{lon}", "", False, nearest_d)
    elif pol == "reject":
        return (None, None, False, nearest_d)
    return (f"{lat},{lon}", "", False, nearest_d)


# --- LIFF Meta Parsing Helpers ---
def _parse_meta_from_address(addr_text: str):
    """Expect: 'Lat:<lat>, Lon:<lon> (txn=<uuid>|acc=<num>|ts=<ms>)' -> dict"""
    if not addr_text:
        return {}
    try:
        start = addr_text.find('(')
        end = addr_text.find(')', start + 1)
        if start == -1 or end == -1:
            return {}
        meta = addr_text[start+1:end]  # txn=...|acc=...|ts=...
        parts = {}
        for kv in meta.split('|'):
            if '=' in kv:
                k, v = kv.split('=', 1)
                parts[k.strip()] = v.strip()
        return parts
    except Exception:
        return {}

def _is_recent_ts_ms(ts_ms: str, max_age_sec: int) -> bool:
    try:
        client_ms = int(ts_ms)
        age = (datetime.now().timestamp()*1000 - client_ms) / 1000.0
        return 0 <= age <= max_age_sec
    except Exception:
        return False

# --- LINE Push Helper ---
def push_text(user_id: str, text: str):
    try:
        line_bot_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[V3TextMessage(text=text)]
            )
        )
    except Exception as e:
        print(f"WARNING: push_text failed: {e}")
        traceback.print_exc()
        sys.stdout.flush()

# --- CheckIns Helpers (row locate / timeout / finalize) ---

def _find_checkins_row_by_id(checkin_id):
    """Return (row_values, row_index_1_based) for given checkin_id in CheckIns sheet; or (None, None)"""
    rows = get_sheet_data("CheckIns")
    for i, r in enumerate(rows):
        if r and len(r) > 0 and r[0] == checkin_id:
            return r, i + 1
    return None, None

# --- Submissions helpers (row locate / upsert / finalize) ---
def _find_submissions_row_by_id(submit_id):
    rows = get_sheet_data(SUBMISSIONS_SHEET_NAME)
    if not rows:
        return None, None
    for i, r in enumerate(rows):
        if r and len(r) > 0 and r[0] == submit_id:
            return r, i + 1
    return None, None

def upsert_submission_row_idempotent(submit_id: str, user_id: str,
                                     location_name: str, site_group: str,
                                     distance_m, employee_name: str = ""):
    existing_row, existing_idx = _find_submissions_row_by_id(submit_id)
    if existing_idx:
        _ensure_row_len(existing_row, 19)  # up to S
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        existing_row[3] = location_name
        existing_row[4] = site_group
        existing_row[8] = ts               # last_updated_at
        existing_row[9] = existing_row[9] or "pending"
        if len(existing_row) > 11:
            existing_row[11] = distance_m  # distance_m (L)
        existing_row[18] = employee_name or (existing_row[18] if len(existing_row) > 18 else "")  # S: employee_name
        _update_row_dynamic(SUBMISSIONS_SHEET_NAME, existing_idx, existing_row)
        return existing_idx
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    base_row = [
        submit_id, ts, user_id, location_name, site_group,
        "", "", "",            # F..H image_url_1..3
        ts, "pending", "",     # I last_updated, J status, K warning_sent
        distance_m             # L distance_m
    ]
    # Extend to include M..R (hashes & duplicate refs) as empty placeholders
    # M..O = image_hash_1..3, P..R = duplicate_of_1..3
    base_row += ["", "", "", "", "", ""]  # M..R (6 empty cells)
    # Finally, add S = employee_name
    new_row = base_row + [employee_name or ""]
    try:
        append_sheet_data(SUBMISSIONS_SHEET_NAME, new_row)
    except Exception as e:
        print(f"WARNING: append Submissions failed once: {e}")
        traceback.print_exc(); sys.stdout.flush()
        chk_row, chk_idx = _find_submissions_row_by_id(submit_id)
        if chk_idx:
            return chk_idx
        append_sheet_data(SUBMISSIONS_SHEET_NAME, new_row)
    final_row, final_idx = _find_submissions_row_by_id(submit_id)
    return final_idx

def _finalize_submission(user_id, submit_id, status_text):
    try:
        row, idx = _find_submissions_row_by_id(submit_id)
        if row and idx:
            _ensure_row_len(row, 12)
            row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row[9] = status_text
            _update_row_dynamic(SUBMISSIONS_SHEET_NAME, idx, row)
    except Exception as e:
        print(f"WARNING: finalize submission failed: {e}")
        traceback.print_exc(); sys.stdout.flush()
    try:
        update_employee_state(user_id, "idle", "")
    except Exception as e:
        print(f"WARNING: finalize submission set idle failed: {e}")
        traceback.print_exc(); sys.stdout.flush()

# --- New idempotent upsert for CheckIns ---
def upsert_checkin_row_idempotent(checkin_id: str, user_id: str,
                                  location_name: str, site_group: str,
                                  distance_m, employee_name: str = ""):
    """
    สร้าง/อัปเดตแถว CheckIns สำหรับ checkin_id แบบ idempotent:
      - เช็คก่อนว่ามีอยู่แล้วหรือยัง
      - ถ้าไม่มี ให้ append 1 ครั้ง
      - ถ้า append timeout ให้ตรวจซ้ำว่าถูกเขียนไปแล้วก่อนค่อยลองครั้งสุดท้าย
    คืนค่า: (row_index_1based หรือ None)
    """
    # 1) มีอยู่แล้วหรือยัง
    existing_row, existing_idx = _find_checkins_row_by_id(checkin_id)
    if existing_idx:
        _ensure_row_len(existing_row, 13)    # up to M
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        existing_row[3] = location_name
        existing_row[4] = site_group
        existing_row[8] = ts                 # last_updated_at
        existing_row[9] = existing_row[9] or "pending"
        if len(existing_row) > 11:
            existing_row[11] = distance_m    # distance_m (L)
        existing_row[12] = employee_name or (existing_row[12] if len(existing_row) > 12 else "")  # employee_name (M)
        _update_row_dynamic("CheckIns", existing_idx, existing_row)
        # remember row index for fast finalize even if read fails later
        _checkins_row_index_cache[checkin_id] = existing_idx
        return existing_idx

    # 2) ยังไม่มี → สร้างแถวใหม่
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_row = [
        checkin_id,
        ts,                 # created_at (B)
        user_id,            # line_user_id (C)
        location_name,      # location_name (D)
        site_group,         # site_group (E)
        "", "", "",         # image_url_1..3 (F..H)
        ts,                 # last_updated_at (I)
        "pending",          # status (J)
        "",                 # warning_sent (K)
        distance_m          # distance_m (L)
    ]
    # Ensure base length then append employee name at M
    _ensure_row_len(new_row, 12)  # up to L
    new_row.append(employee_name or "")  # M: employee_name

    try:
        append_sheet_data("CheckIns", new_row)
    except Exception as e:
        print(f"WARNING: append CheckIns failed once: {e}")
        traceback.print_exc(); sys.stdout.flush()
        # ตรวจซ้ำว่าเขียนไปแล้วหรือยัง
        chk_row, chk_idx = _find_checkins_row_by_id(checkin_id)
        if chk_idx:
            return chk_idx
        # ยังไม่เจอจริง ๆ → ลองครั้งสุดท้าย
        append_sheet_data("CheckIns", new_row)

    # 3) หา index ที่แท้จริงหลัง append สำเร็จ
    final_row, final_idx = _find_checkins_row_by_id(checkin_id)
    if final_idx:
        _checkins_row_index_cache[checkin_id] = final_idx
    return final_idx

def _count_images_in_row(row):
    """Count non-empty image_url_1..3 in row (F..H -> indices 5..7)"""
    cnt = 0
    for j in range(5, 8):
        if j < len(row) and row[j]:
            cnt += 1
    return cnt

def _first_empty_image_slot_index(row, start_col=5, end_col=7):
    """
    Return the first empty image slot index in row among columns F..H (0-based 5..7).
    If all filled, return None.
    """
    for j in range(start_col, end_col + 1):
        if j >= len(row) or not row[j]:
            return j
    return None

def _update_checkins_add_image_url(checkin_id: str, image_url: str):
    """
    Idempotently ใส่ image_url ลงช่องรูปว่างช่องแรก (F..H) ของแถว CheckIns ของ checkin_id
    และอัปเดต I: last_updated_at, J: status='in_progress' โดยไม่ append แถวใหม่
    """
    lock = _txn_locks[checkin_id]
    with lock:
        row, idx = _find_checkins_row_by_id(checkin_id)
        if not idx:
            # หากยังไม่มีแถว (กรณี edge) ให้ upsert ก่อน
            upsert_checkin_row_idempotent(checkin_id, "", "", "", 0, "")
            row, idx = _find_checkins_row_by_id(checkin_id)
            if not idx:
                raise RuntimeError(f"Cannot locate CheckIns row for {checkin_id}")

        # cache row index for this checkin to support finalize without a fresh read
        _checkins_row_index_cache[checkin_id] = idx

        _ensure_row_len(row, 12)  # A..L
        slot = _first_empty_image_slot_index(row, 5, 7)
        if slot is None:
            # ครบ 3 ช่องแล้ว แค่รีเฟรชเวลา/สถานะ
            row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # I
            row[9] = row[9] or "in_progress"                       # J
            _update_row_dynamic("CheckIns", idx, row)
            _checkins_row_index_cache[checkin_id] = idx
            return idx, 3

        row[slot] = image_url
        row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      # I
        # Keep terminal statuses; only advance to in_progress if not already finalized
        curr_status = (row[9].strip().lower() if len(row) > 9 and row[9] else "")
        if curr_status not in ("done", "timeout", "cancelled"):
            row[9] = "in_progress"                                 # J
        _update_row_dynamic("CheckIns", idx, row)
        _checkins_row_index_cache[checkin_id] = idx
        filled = _count_images_in_row(row)
        return idx, filled

def _update_submissions_add_image_url(submit_id: str, image_url: str, image_hash_hex: str):
    """
    สำหรับ Submissions: ใส่รูปลง F..H, เก็บแฮชลง M..O, ถ้าพบซ้ำให้จด reference ลง P..R
    ทำแบบ idempotent ต่อช่อง ไม่สร้างแถวใหม่
    """
    lock = _txn_locks[submit_id]
    with lock:
        row, idx = _find_submissions_row_by_id(submit_id)
        if not idx:
            # Do NOT auto-create a new row here; it would reset distance_m to 0.
            # The row must already exist from the location step.
            raise RuntimeError(f"Cannot locate Submissions row for {submit_id} (expected to be created at location step)")

        _ensure_row_len(row, 19)  # ถึง S: employee_name
        slot = _first_empty_image_slot_index(row, 5, 7)  # F..H
        if slot is None:
            row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # I
            row[9] = row[9] or "in_progress"                       # J
            _update_row_dynamic(SUBMISSIONS_SHEET_NAME, idx, row)
            return idx, 3, None

        # ใส่ URL
        row[slot] = image_url
        # เขียนแฮชลง M..O (12..14)
        hash_col = 12 + (slot - 5)
        if image_hash_hex:
            row[hash_col] = image_hash_hex

        # ตรวจซ้ำย้อนหลัง (ยกเว้น submit นี้เอง)
        dup_submit_id, dup_row_idx, dup_slot_1based = _find_duplicate_in_submissions(image_hash_hex, exclude_submit_id=submit_id)
        dup_note = None
        if dup_submit_id and dup_slot_1based:
            # Map slot to image URL column letters F,G,H (1->F, 2->G, 3->H)
            col_letter_map = {1: "F", 2: "G", 3: "H"}
            col_letter = col_letter_map.get(dup_slot_1based, "?")
            # Write only "row & column" info, e.g., "row 12 col F"
            dup_note = f"row {dup_row_idx} col {col_letter}"
            dup_col = 15 + (slot - 5)  # P..R for current record's duplicate_of_1..3
            row[dup_col] = dup_note

        row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      # I
        # Keep terminal statuses; only set to in_progress if not finalized
        curr_status = (row[9].strip().lower() if len(row) > 9 and row[9] else "")
        if curr_status not in ("done", "timeout", "cancelled"):
            row[9] = "in_progress"                                  # J
        _update_row_dynamic(SUBMISSIONS_SHEET_NAME, idx, row)
        filled = _count_images_in_row(row)
        return idx, filled, dup_note
    
def _ensure_row_len(row, length):
    """Pad row with empty strings to at least 'length' items (generic)."""
    while len(row) < length:
        row.append("")
    return row

def _finalize_checkin(user_id, checkin_id, status_text, reply_token=None, send_summary=False):
    """Set CheckIns status first, then Employees state. Each with retries and isolation from timeouts.
    Uses a per-transaction lock to avoid race conditions with concurrent image updates
    (which also acquire the same lock in _update_checkins_add_image_url).

    If send_summary=True and status_text == "done", reply/push a concise summary to the user indicating
    how many images were saved.
    """
    # Acquire the same per-transaction lock used by image writes to prevent status clobbering
    lock = _txn_locks[checkin_id]
    with lock:
        # Try to locate row; if read fails, fall back to cached row index
        try:
            row, idx = _find_checkins_row_by_id(checkin_id)
        except Exception:
            row, idx = None, _checkins_row_index_cache.get(checkin_id)

        # Update status & timestamp
        try:
            last_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if idx:
                # Minimal range update when we know the row index
                update_sheet_data("CheckIns", f"CheckIns!I{idx}:J{idx}", [last_ts, status_text])
            else:
                # Fallback: full read/update
                row2, idx2 = _find_checkins_row_by_id(checkin_id)
                if row2 and idx2:
                    _ensure_row_len(row2, 12)
                    row2[8] = last_ts
                    row2[9] = status_text
                    _update_row_dynamic("CheckIns", idx2, row2)
                    _checkins_row_index_cache[checkin_id] = idx2
                    row, idx = row2, idx2
        except Exception as e:
            print(f"WARNING: finalize: failed to update CheckIns for {checkin_id}: {e}")
            traceback.print_exc()
            sys.stdout.flush()

        # Prepare optional summary text (only when finishing as 'done' and requested)
        summary_text = None
        if send_summary and status_text == "done":
            try:
                # Ensure we have the latest row to count images
                if not row or not idx:
                    row, idx = _find_checkins_row_by_id(checkin_id)
                images_now = _count_images_in_row(row) if row else 0
                if images_now >= 3:
                    summary_text = "เช็คอินเรียบร้อย ✅ ส่งภาพครบ 3 ภาพ"
                elif images_now > 0:
                    summary_text = f"เช็คอินเสร็จสิ้น ✅ ส่งภาพมาแล้ว {images_now} รูป"
                else:
                    summary_text = "เช็คอินเรียบร้อย ✅ (ไม่ได้แนบภาพ)"
            except Exception as e:
                print(f"WARNING: finalize: summary compose failed for {checkin_id}: {e}")
                traceback.print_exc()
                sys.stdout.flush()

    # Update Employees state (separate try to avoid blocking the scheduler)
    try:
        update_employee_state(user_id, "idle", "")
    except Exception as e:
        print(f"WARNING: finalize: failed to set employee {user_id} idle: {e}")
        traceback.print_exc()
        sys.stdout.flush()

    # Send summary to the user if requested
    if send_summary and summary_text:
        try:
            if reply_token:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[V3TextMessage(text=summary_text)]
                    )
                )
            else:
                push_text(user_id, summary_text)
        except Exception as e:
            print(f"WARNING: finalize: failed to send summary message: {e}")
            traceback.print_exc()
            sys.stdout.flush()

def _check_and_handle_timeout(user_id, reply_token=None):
    """On any incoming event, check remaining time.
    - If within WARNING_BEFORE_SECONDS and not warned yet -> send warning + mark K.
    - If timed out -> finalize and notify.
    Return True if the function handled a terminal timeout, else False.
    """
    employee_data, _ = get_employee_data(user_id)
    if not employee_data:
        return False
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL]
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL]
    if current_state != "waiting_for_checkin_images" or not current_transaction_id:
        return False

    # Load the corresponding CheckIns row
    row, idx = _find_checkins_row_by_id(current_transaction_id)
    if not row:
        return False
    _ensure_row_len(row, 12)  # A..L

    # Determine last activity time: use I (index 8) if present, else B (index 1)
    last_ts_str = row[8] or (row[1] if len(row) > 1 else "")
    if not last_ts_str:
        return False
    try:
        last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
    except Exception:
        return False

    now_dt = datetime.now()
    elapsed = (now_dt - last_ts).total_seconds()
    seconds_left = CHECKIN_TIMEOUT_SECONDS - elapsed

    # Pre-timeout warning on event path
    warned = (len(row) > 10 and str(row[10]).strip() != "")
    if 0 < seconds_left <= WARNING_BEFORE_SECONDS and not warned:
        row[9] = "warning"  # J: status
        row[10] = "1"        # K: warning_sent
        row[8] = now_dt.strftime('%Y-%m-%d %H:%M:%S')  # refresh last_updated_at so we don't double-warn too fast
        update_sheet_data("CheckIns", f"CheckIns!A{idx}:L{idx}", row)
        # Prefer reply if we have a reply_token for this event; otherwise push
        try:
            if reply_token:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[V3TextMessage(text=f"จะหมดเวลาใน {int(max(1, round(seconds_left)))} วินาที กรุณาส่งรูปให้ครบ 3 รูป หรือพิมพ์ 'จบ'")]
                    )
                )
            else:
                push_text(user_id, f"จะหมดเวลาใน {int(max(1, round(seconds_left)))} วินาที กรุณาส่งรูปให้ครบ 3 รูป หรือพิมพ์ 'จบ'")
        except Exception:
            pass
        print(f"DEBUG: Event-path warning sent, seconds_left={seconds_left:.1f}")
        sys.stdout.flush()
        # Do not return here; allow further processing of the current event

    if seconds_left <= 0:
        # finalize as timeout
        _finalize_checkin(user_id, current_transaction_id, "timeout")
        notify_text = f"หมดเวลา {CHECKIN_TIMEOUT_SECONDS} วินาที ระบบปิดเช็คอินให้อัตโนมัติแล้วครับ"
        try:
            if reply_token:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=reply_token,
                        messages=[V3TextMessage(text=notify_text)]
                    )
                )
            else:
                push_text(user_id, notify_text)
        except Exception:
            pass
        print(f"DEBUG: Check-in timed out for user {user_id}, transaction {current_transaction_id}")
        sys.stdout.flush()
        return True

    return False

# --- Background Job: scan and timeout overdue check-ins ---
def _scan_and_timeout_overdue_checkins():
    start_ts = time.time()
    print("DEBUG: Scheduler run start"); sys.stdout.flush()
    try:
        # Read once per run
        employees = get_sheet_data("Employees")
        if not employees:
            print("DEBUG: Scheduler: no Employees data; skip run"); sys.stdout.flush()
            return

        now_dt = datetime.now()

        # Build employee index: line_id -> (row, idx1)
        emp_index = {}
        for i, r in enumerate(employees):
            if r and len(r) > 0 and r[0]:
                emp_index[r[0]] = (r, i + 1)

        # Fast exit if no one is waiting for images
        any_waiting = False
        for r, _ in emp_index.values():
            state = r[EMPLOYEE_CURRENT_STATE_COL] if len(r) > EMPLOYEE_CURRENT_STATE_COL else ""
            if state == "waiting_for_checkin_images":
                any_waiting = True
                break
        if not any_waiting:
            print("DEBUG: Scheduler early-exit: no employees waiting for images"); sys.stdout.flush()
            return

        # Read CheckIns only when needed (use quick-read to avoid long blocking)
        quick_to = min(SHEETS_EXECUTE_TIMEOUT_SEC, max(5, SCHEDULER_INTERVAL_SECONDS - 1))
        checkins = get_sheet_data_quick("CheckIns", timeout_sec=quick_to)
        if checkins is None:
            print("DEBUG: Scheduler: quick read CheckIns failed; skip run"); sys.stdout.flush()
            return

        for ci, row in enumerate(checkins):
            if not row or len(row) < 3:
                continue
            checkin_id = row[0]
            line_id = row[2]
            status = row[9] if len(row) > 9 else ""
            if status in ("done", "timeout", "cancelled"):
                continue

            last_ts_str = row[8] if len(row) > 8 and row[8] else (row[1] if len(row) > 1 else "")
            if not last_ts_str:
                continue
            try:
                last_dt = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
            except Exception:
                continue

            elapsed = (now_dt - last_dt).total_seconds()
            seconds_left = CHECKIN_TIMEOUT_SECONDS - elapsed

            # warning window
            warned = (len(row) > 10 and str(row[10]).strip() != "")
            if 0 < seconds_left <= WARNING_BEFORE_SECONDS and not warned:
                _ensure_row_len(row, 12)  # A..L
                row[9] = "warning"           # J
                row[10] = "1"                # K
                row[8] = now_dt.strftime('%Y-%m-%d %H:%M:%S')  # refresh last_updated_at
                _update_row_dynamic("CheckIns", ci+1, row)
                try:
                    push_text(line_id, f"จะหมดเวลาใน {int(max(1, round(seconds_left)))} วินาที กรุณาส่งรูปให้ครบ 3 รูป หรือพิมพ์ 'จบ'")
                except Exception:
                    pass
                continue

            if seconds_left > 0:
                continue

            emp_tuple = emp_index.get(line_id)
            if not emp_tuple:
                continue
            emp_row, emp_row_idx = emp_tuple
            while len(emp_row) <= EMPLOYEE_CURRENT_TRANSACTION_ID_COL:
                emp_row.append("")
            state = emp_row[EMPLOYEE_CURRENT_STATE_COL] if len(emp_row) > EMPLOYEE_CURRENT_STATE_COL else ""
            txn = emp_row[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if len(emp_row) > EMPLOYEE_CURRENT_TRANSACTION_ID_COL else ""
            if state != "waiting_for_checkin_images" or txn != checkin_id:
                continue

            print(f"DEBUG: Scheduler timing out checkin {checkin_id} for user {line_id} (elapsed={elapsed}s)")
            sys.stdout.flush()
            _finalize_checkin(line_id, checkin_id, "timeout")
            try:
                push_text(line_id, f"หมดเวลา {CHECKIN_TIMEOUT_SECONDS} วินาที ระบบปิดเช็คอินให้อัตโนมัติแล้วครับ")
            except Exception:
                pass
    except Exception as e:
        print(f"ERROR: _scan_and_timeout_overdue_checkins failed: {e}")
        traceback.print_exc()
        sys.stdout.flush()
    finally:
        dur = time.time() - start_ts
        print(f"DEBUG: Scheduler run end (took {dur:.2f}s)"); sys.stdout.flush()

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
        traceback.print_exc()
        sys.stdout.flush()
        abort(400)
    except Exception as e:
        print(f"ERROR: Error handling webhook: {e}")
        print(f"Exception details: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        abort(500)

    return 'OK'

# --- OAuth Routes ---
@app.route('/authorize')
def authorize():
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, 
        scopes=OAUTH_SCOPES)
    flow.redirect_uri = GOOGLE_REDIRECT_URI

    authorization_url, state = flow.authorization_url(
        access_type='offline',            # Required to get a refresh token
        include_granted_scopes='true',
        prompt='consent'                  # Force consent to reliably obtain refresh_token on first auth
    )
    
    session['oauth_state'] = state
    print(f"DEBUG: set session oauth_state = {state}") # New print
    sys.stdout.flush()
    return redirect(authorization_url)

@app.route('/oauth2callback')
def oauth2callback():
    state = session.get('oauth_state')
    if not state:
        return "Invalid or missing OAuth state. Please start authorization again at /authorize", 400

    print(f"DEBUG: session oauth_state = {session.get('oauth_state')}") # New print
    print(f"DEBUG: query state = {request.args.get('state')}") # New print
    print(f"DEBUG: query code  = {request.args.get('code')}") # New print
    print(f"DEBUG: request url   = {request.url}") # New print
    sys.stdout.flush()

    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, 
        scopes=OAUTH_SCOPES, state=state)
    flow.redirect_uri = GOOGLE_REDIRECT_URI

    authorization_response = request.url
    flow.fetch_token(authorization_response=authorization_response)

    credentials = flow.credentials

    # Save the credentials for the next run
    with open(TOKEN_PATH, 'w') as f:
        f.write(credentials.to_json())
    
    # Now, you can get the folder ID from the user's Drive
    # For simplicity, we'll assume a specific folder name or ID will be set manually after auth.
    # In a real app, you might prompt the user to select a folder or create one.
    # For now, we'll just confirm success.
    return 'Authorization successful! You can close this tab.'


# --- LIFF App Serving Route ---

@app.route("/liff_location_picker")
def liff_location_picker():
    """
    LIFF page to acquire GPS from the mobile and send a LINE location message back to chat.
    It reads ?txn=&lt;uuid&gt; and embeds (txn|acc|ts) into the address field for anti-fraud.

    Changes in this version:
    - Fixes the "ต้องกด 2 ครั้ง" issue by auto-resuming after LIFF login using sessionStorage('autoRun').
    - Single-tap flow: first tap → login (if needed) → auto-continue to read GPS → send location.
    - Keeps anti-fraud metadata (txn|acc|ts) and accuracy gating.
    - Styling: center primary button; smaller secondary buttons.
    NOTE: This version avoids Python f-strings to prevent conflicts with `{}` in HTML/CSS/JS.
    """
    if not LIFF_ID:
        return "LIFF_ID is not set on server", 500

    txn = request.args.get("txn", "")

    # Use literal placeholders and replace later to avoid f-string brace escaping issues.
    html = """<!doctype html>
<html lang="th">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
  <title>ส่งตำแหน่งเช็คอิน</title>
  <script src="https://static.line-scdn.net/liff/edge/2/sdk.js"></script>
  <style>
    :root {
      --bg: #0b0f14;
      --card: #121922;
      --text: #e8eef6;
      --muted: #9fb2c8;
      --accent: #2dd4bf;
      --accent-press: #22b3a0;
      --danger: #f87171;
      --warn: #fbbf24;
      --ok: #86efac;
      --ring: rgba(45, 212, 191, .3);
    }
    @media (prefers-color-scheme: light) {
      :root {
        --bg: #f7fafc;
        --card: #ffffff;
        --text: #0f172a;
        --muted: #516070;
        --accent: #0ea5e9;
        --accent-press: #0284c7;
        --danger: #dc2626;
        --warn: #d97706;
        --ok: #16a34a;
        --ring: rgba(14, 165, 233, .2);
      }
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      margin: 0;
      background: var(--bg);
      color: var(--text);
      display: grid;
      place-items: center;
      padding: 16px;
    }
    .container { width: 100%; max-width: 520px; }
    .card {
      background: var(--card);
      border-radius: 16px;
      padding: 20px;
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
      border: 1px solid rgba(255,255,255,.06);
    }
    .header { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 0 6px var(--ring); }
    h1 { font-size: 18px; margin: 0; }
    p.lead { margin: 6px 0 16px 0; color: var(--muted); line-height: 1.5; }
    .status {
      display: grid; gap: 8px;
      background: rgba(255,255,255,.04);
      border: 1px dashed rgba(255,255,255,.12);
      border-radius: 12px;
      padding: 12px; margin-bottom: 16px; font-size: 14px;
    }
    .row { display: flex; justify-content: space-between; gap: 12px; }
    .row span:first-child { color: var(--muted); }
    .actions { display: flex; flex-direction: column; align-items: center; gap: 12px; margin-top: 10px; }
    .btnwrap { display: flex; justify-content: center; width: 100%; }
    button.primary {
      appearance: none; border: none; cursor: pointer;
      padding: 14px 16px; border-radius: 12px;
      font-weight: 600; font-size: 16px;
      background: var(--accent); color: #001418;
      transition: transform .02s ease, background .2s ease, box-shadow .2s ease;
      box-shadow: 0 10px 25px var(--ring);
      width: 100%; max-width: 320px;
    }
    button.primary:active { transform: translateY(1px); background: var(--accent-press); }
    button.secondary {
      appearance: none; border: 1px solid rgba(255,255,255,.18); cursor: pointer;
      padding: 8px 12px; border-radius: 10px;
      font-weight: 600; font-size: 13px;
      background: transparent; color: var(--text); opacity: .9;
    }
    .helper { margin-top: 10px; font-size: 13px; color: var(--muted); text-align: center; }
    .hint { margin-top: 10px; font-size: 13px; color: var(--warn); text-align: center; }
    .ok { color: var(--ok); } .danger { color: var(--danger); }
    .log { display:none; white-space: pre-wrap; }
    .spinner { display: none; width: 20px; height: 20px; border-radius: 50%; border: 3px solid rgba(255,255,255,.25); border-top-color: var(--accent); animation: spin 0.8s linear infinite; margin-left: 8px; }
    footer { margin-top: 14px; text-align: center; font-size: 12px; color: var(--muted); }
    a { color: inherit; }
    #openInLineBtn { display:none; }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <div class="header">
        <div class="dot"></div><h1>ส่งตำแหน่งเช็คอิน</h1>
      </div>
      <p class="lead">กดปุ่มด้านล่างเพื่อส่ง <b>ตำแหน่ง Location จากมือถือ</b></p>

      <div class="status" id="statusBox">
        <div class="row"><span>สถานะ LIFF</span><span id="stLiff" class="danger">ยังไม่เริ่ม</span></div>
        <div class="row"><span>สิทธิ์ตำแหน่ง (GPS)</span><span id="stPerm" class="danger">ยังไม่ได้ร้องขอ</span></div>
        <div class="row"><span>ความแม่นยำ (m)</span><span id="stAcc">-</span></div>
        <div class="row"><span>เวลาที่อ่าน</span><span id="stTs">-</span></div>
      </div>

      <div class="actions">
        <div class="btnwrap">
          <button id="sendBtn" class="primary">ส่งตำแหน่งปัจจุบัน</button>
          <div class="spinner" id="spin"></div>
        </div>
        <button id="retryBtn" class="secondary">ลองใหม่ / รีเฟรชสิทธิ์</button>
        <button id="openInLineBtn" class="secondary">เปิดใน LINE แล้วลองอีกครั้ง</button>
      </div>

      <p class="helper">เปิด GPS/Location, เปิดสัญญาณมือถือ/ไวไฟ และยืนกลางแจ้งเพื่อความแม่นยำที่ดีกว่า</p>
      <p class="hint" id="hint"></p>
      <pre class="log" id="log"></pre>
      <footer>LIFF ID: <span id="liffIdLabel">-</span> · Txn: <span id="txnLabel">-</span></footer>
    </div>
  </div>

    <script>
    const LIFF_ID = "__LIFF_ID__";
    const txn = "__TXN__";
    const MAX_ACC = __MAX_ACC__;

    const stLiff = document.getElementById('stLiff');
    const stPerm = document.getElementById('stPerm');
    const stAcc = document.getElementById('stAcc');
    const stTs = document.getElementById('stTs');
    const hint = document.getElementById('hint');
    const spin = document.getElementById('spin');
    const sendBtn = document.getElementById('sendBtn');
    const retryBtn = document.getElementById('retryBtn');
    const openInLineBtn = document.getElementById('openInLineBtn');
    const liffIdLabel = document.getElementById('liffIdLabel');
    const txnLabel = document.getElementById('txnLabel');

    // Ensure single-run to avoid "ต้องกดสองครั้ง"
    let __started = false;
    function startOnce() {
      if (__started) return;
      __started = true;
      acquireAndSend();
    }

    liffIdLabel.textContent = LIFF_ID || "-";
    txnLabel.textContent = txn || "-";

    function setBusy(b) {
      sendBtn.disabled = b;
      retryBtn.disabled = b;
      openInLineBtn.disabled = b;
      spin.style.display = b ? 'inline-block' : 'none';
    }

    function fmtTs(ms) { try { return new Date(ms).toLocaleString(); } catch { return "-"; } }

    async function initLiff(autoLogin=false) {
      try {
        await liff.init({ liffId: LIFF_ID });
        if (!liff.isLoggedIn()) {
          stLiff.textContent = "กำลังเข้าสู่ระบบ…";
          if (autoLogin) {
            // Mark to auto-continue after redirect
            sessionStorage.setItem("autoRun", "1");
            liff.login({ redirectUri: window.location.href });
          }
          return false;
        }
        if (!liff.isInClient()) {
          stLiff.textContent = "เปิดนอกแอป LINE";
          stLiff.className = "danger";
          hint.textContent = "กรุณาเปิดหน้านี้จากในแอป LINE เพื่อส่งข้อความเข้าแชต";
          openInLineBtn.style.display = "inline-block";
          return true; // allow viewing but cannot send
        }
        stLiff.textContent = "พร้อมใช้งาน";
        stLiff.className = "ok";
        return true;
      } catch (e) {
        stLiff.textContent = "เริ่ม LIFF ไม่สำเร็จ";
        stLiff.className = "danger";
        hint.textContent = (e && e.message) ? e.message : String(e);
        alert("ไม่สามารถเริ่ม LIFF ได้: " + hint.textContent);
        return false;
      }
    }

    async function acquireAndSend() {
      const ok = await initLiff(true);  // autoLogin enabled
      if (!ok) return;

      if (!navigator.geolocation) {
        stPerm.textContent = "อุปกรณ์ไม่รองรับ";
        stPerm.className = "danger";
        alert("อุปกรณ์ไม่รองรับการขอตำแหน่ง");
        return;
      }

      setBusy(true);
      hint.textContent = "กำลังอ่านพิกัด… โปรดอย่าปิดหน้านี้";

      navigator.geolocation.getCurrentPosition(async (pos) => {
        try {
          const lat = pos.coords.latitude;
          const lon = pos.coords.longitude;
          const acc = Math.round(pos.coords.accuracy || 0);
          const ts  = Date.now();

          stPerm.textContent = "อนุญาตแล้ว";
          stPerm.className = "ok";
          stAcc.textContent = acc;
          stTs.textContent = fmtTs(ts);

          if (acc > MAX_ACC) {
            hint.textContent = "ความแม่นยำ " + acc + "m สูงเกินไป ลองเปิด GPS ให้แม่นขึ้นหรือย้ายไปกลางแจ้ง";
          } else {
            hint.textContent = "";
          }

          if (!liff.isInClient()) {
            alert("ขณะนี้เปิดในเบราว์เซอร์ภายนอก ไม่สามารถส่งเข้าห้องแชตได้ กรุณากด 'เปิดใน LINE แล้วลองอีกครั้ง'");
            return;
          }

          const addressMeta = `(txn=${txn}|acc=${acc}|ts=${ts})`;
          await liff.sendMessages([{ type: "location", title: "ตำแหน่งของฉัน", address: addressMeta, latitude: lat, longitude: lon }]);

          alert("ส่งตำแหน่งแล้ว ✔ กลับไปที่แชตได้เลย");
          liff.closeWindow();
        } catch (err) {
          alert("ส่งตำแหน่งไม่สำเร็จ: " + (err && err.message ? err.message : err));
        } finally {
          setBusy(false);
        }
      }, (err) => {
        stPerm.textContent = "ปฏิเสธสิทธิ์/ล้มเหลว";
        stPerm.className = "danger";
        hint.textContent = (err && err.message) ? err.message : String(err);
        setBusy(false);
        alert("ไม่ได้รับสิทธิ์ตำแหน่ง: " + ((err && err.message) ? err.message : err));
      }, { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 });
    }

    function openInLine() {
      const link = `line://app/${LIFF_ID}?txn=${encodeURIComponent(txn)}`;
      window.location.href = link;
    }

    document.addEventListener('DOMContentLoaded', async () => {
      // Wire buttons
      sendBtn.addEventListener('click', startOnce);
      retryBtn.addEventListener('click', () => { hint.textContent = ""; __started = false; startOnce(); });
      openInLineBtn.addEventListener('click', openInLine);

      // Initialize LIFF and try zero-tap start if possible
      await initLiff(true);

      // If we just returned from login, auto-continue without another tap
      if (sessionStorage.getItem("autoRun") === "1") {
        sessionStorage.removeItem("autoRun");
        startOnce();
        return;
      }

      // Already logged-in and inside LINE client? auto-start right away
      try {
        if (typeof liff !== 'undefined' && liff.isLoggedIn && liff.isLoggedIn() && liff.isInClient && liff.isInClient()) {
          startOnce();
        }
      } catch (e) {
        // ignore; user can tap the button
      }
    });
  </script>
</body>
</html>"""

    # Safe placeholder substitution (no f-string)
    html = (html
            .replace("__LIFF_ID__", LIFF_ID)
            .replace("__TXN__", txn)
            .replace("__MAX_ACC__", str(MAX_GPS_ACCURACY_M)))

    return render_template_string(html)

@app.route("/liff-location")
def liff_location_alias():
    """Alias for older Endpoint URLs; serves the same page as /liff_location_picker."""
    return liff_location_picker()

@app.route("/", methods=["GET", "HEAD"])
def root_ok():
    """Simple health check to avoid 502 on root requests."""
    return "OK", 200

@app.route("/favicon.ico")
def favicon_noop():
    """Return 204 for favicon to prevent 404/502 noise."""
    return Response(status=204)

#
# --- Helper: tolerant "finish" command detection ---
def _is_finish_checkin_text(txt: str) -> bool:
    """
    Return True if text indicates finishing the check-in, allowing common typos.
    Accepts exact phrases and tolerant patterns like 'จบ ... เช็คอิน/เชคอิน/เช็คอืน/เชคอืน'.
    """
    t = (txt or "").strip().lower()
    # exact matches first
    exact = ("จบ", "จบการเช็คอิน", "จบเช็คอิน", "yes", "y", "done", "finish", "เสร็จแล้ว", "จบลงทะเบียน")
    if t in exact:
        return True
    # tolerant: has 'จบ' and any variant of 'เช็คอิน'
    variants = ("เช็คอิน", "เชคอิน", "เช็คอืน", "เชคอืน")
    if "จบ" in t and any(v in t for v in variants):
        return True
    return False

def _is_finish_submit_text(txt: str) -> bool:
    """
    Return True if text indicates finishing the submission, allowing common phrasing.
    """
    t = (txt or "").strip().lower()
    exact = ("จบ", "จบการส่งงาน", "yes", "y", "done", "finish", "ส่งงานเสร็จ", "เสร็จแล้ว")
    if t in exact:
        return True
    # tolerant: has 'จบ' and 'ส่งงาน' or 'งาน'
    if "จบ" in t and ("ส่งงาน" in t or "งาน" in t):
        return True
    return False

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
    if employee_data == "__SHEETS_ERROR__":
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ระบบกำลังเชื่อมต่อ Google Sheets ช้ากว่าปกติ กรุณาลองอีกครั้งในสักครู่ครับ")]
            )
        )
        print("DEBUG: Reply temporary: Sheets unavailable; not treating as unregistered.")
        sys.stdout.flush()
        return
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if employee_data else "idle"
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if employee_data else ""

    print(f"DEBUG: User {user_id} current_state: {current_state}, current_transaction_id: {current_transaction_id}")
    sys.stdout.flush()

    # Auto-timeout check (handles finalize and reply if needed)
    if _check_and_handle_timeout(user_id, reply_token=event.reply_token):
        return

    # --- User cancels current check-in or submission ---
    if text in ("ยกเลิก", "cancel", "ยกเลิกเช็คอิน", "ยกเลิกส่งงาน"):
        if current_transaction_id and current_state in ("waiting_for_checkin_location", "waiting_for_checkin_images"):
            _finalize_checkin(user_id, current_transaction_id, "cancelled")
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="ยกเลิกเช็คอินให้แล้วครับ")]
                )
            )
            print(f"DEBUG: User cancelled check-in. transaction_id={current_transaction_id}"); sys.stdout.flush(); return
        if current_transaction_id and current_state in ("waiting_for_submit_location", "waiting_for_submit_images"):
            _finalize_submission(user_id, current_transaction_id, "cancelled")
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="ยกเลิกการส่งงานให้แล้วครับ")]
                )
            )
            print(f"DEBUG: User cancelled submission. transaction_id={current_transaction_id}"); sys.stdout.flush(); return
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ตอนนี้ไม่มีรายการที่ค้างอยู่")]
            )
        )
        print("DEBUG: Cancel requested but no active flow."); sys.stdout.flush(); return

    # Start check-in flow on text command
    if text in ("เช็คอิน", "checkin"):
        # Create a new transaction now, store in Employees, and provide LIFF link to capture GPS with metadata
        transaction_id = str(uuid.uuid4())
        update_employee_state(user_id, "waiting_for_checkin_location", transaction_id)

        # Build LIFF URL (fall back to plain instruction if LIFF_ID not set)
        if LIFF_ID:
            liff_url = f"https://liff.line.me/{LIFF_ID}?txn={transaction_id}"
            reply_msg = V3TextMessage(
                text="กดปุ่มด้านล่างเพื่อส่งตำแหน่งจากมือถือ หรือยกเลิก",
                quick_reply=QuickReply(items=[
                    QuickReplyItem(action=URIAction(label="ส่งตำแหน่งปัจจุบัน", uri=liff_url)),
                    QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิก"))
                ])
            )
        else:
            reply_msg = V3TextMessage(
                text="โปรดส่งตำแหน่ง (Location) เพื่อเช็คอิน หรือกดยกเลิก",
                quick_reply=QuickReply(items=[
                    QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิก"))
                ])
            )

        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[reply_msg]
            )
        )
        print(f"DEBUG: Replied: ask for Location via {'LIFF' if LIFF_ID else 'plain location'}, transaction_id={transaction_id}")
        sys.stdout.flush()
        return

    # --- Start submission flow on text command ---
    elif text in ("ส่งงาน", "submit", "ส่งงานนะ"):
        # Start SUBMISSION flow: ask user to share location via LIFF first
        transaction_id = str(uuid.uuid4())
        # Reuse the current-transaction field to track submission flow (separate state)
        update_employee_state(user_id, "waiting_for_submit_location", transaction_id)

        # Build LIFF URL (same LIFF, different purpose). We pass txn only; server state decides semantics.
        if LIFF_ID:
            liff_url = f"https://liff.line.me/{LIFF_ID}?txn={transaction_id}"
            reply_msg = V3TextMessage(
                text="กรุณากดปุ่มเพื่อแชร์ตำแหน่งของคุณก่อนส่งงาน",
                quick_reply=QuickReply(items=[
                    QuickReplyItem(action=URIAction(label="ส่งตำแหน่งส่งงาน", uri=liff_url)),
                    QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิกส่งงาน"))
                ])
            )
        else:
            reply_msg = V3TextMessage(
                text="โปรดส่งตำแหน่ง (Location) เพื่อเริ่มส่งงาน หรือกดยกเลิก",
                quick_reply=QuickReply(items=[
                    QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิกส่งงาน"))
                ])
            )

        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[reply_msg]
            )
        )
        print(f"DEBUG: Replied: ask for Submission Location via {'LIFF' if LIFF_ID else 'plain location'}, transaction_id={transaction_id}")
        sys.stdout.flush()
        return

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

    # --- ผู้ใช้สั่ง "จบ" ให้ปิดงานเป็น done ---
    finish_words_checkin = ("จบ", "จบการเช็คอิน", "จบเช็คอิน", "yes", "y", "done", "finish", "เสร็จแล้ว", "จบลงทะเบียน")
    finish_words_submit  = ("จบ", "จบการส่งงาน", "yes", "y", "done", "finish", "ส่งงานเสร็จ", "เสร็จแล้ว")

    if current_state == "waiting_for_checkin_images" and current_transaction_id and _is_finish_checkin_text(text):
        _finalize_checkin(user_id, current_transaction_id, "done", reply_token=event.reply_token, send_summary=True)
        print(f"DEBUG: User finished early via quick menu. transaction_id={current_transaction_id}")
        sys.stdout.flush()
        return

    if current_state == "waiting_for_submit_images" and current_transaction_id and _is_finish_submit_text(text):
        row, idx = _find_submissions_row_by_id(current_transaction_id)
        images_now = _count_images_in_row(row) if row else 0
        _finalize_submission(user_id, current_transaction_id, "done")
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text=f"บันทึกการส่งงานเรียบร้อย ✅ ได้รับรูปทั้งหมด {images_now} รูป")]
            )
        )
        print(f"DEBUG: Submission finished early. images_now={images_now}, transaction_id={current_transaction_id}")
        sys.stdout.flush()
        return

    if current_state == "waiting_for_checkin_images" and current_transaction_id:
        # We are waiting for images, but user sent TEXT. Ask for images instead.
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    V3TextMessage(
                        text="กำลังรอรูปหลักฐาน โปรดส่งรูปภาพ (สูงสุด 3 รูป)",
                        quick_reply=QuickReply(items=[
                            QuickReplyItem(action=CameraAction(label="📸 ถ่ายภาพเช็คอิน")),
                            QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                            QuickReplyItem(action=MessageAction(label="✅ จบการเช็คอิน", text="จบการเช็คอิน")),
                        ]),
                    )
                ]
            )
        )
        print("DEBUG: Replied: waiting_for_checkin_images → user sent TEXT (ask for image).")
        sys.stdout.flush()
        return

    # Default reply for other texts
    line_bot_api.reply_message(
        ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[V3TextMessage(text="พิมพ์ “เช็คอิน” หรือ “ส่งงาน” เพื่อเริ่มต้น หรือส่งตำแหน่ง (Location) ได้เลย")]
        )
    )
    print("DEBUG: Replied: default text help.")
    sys.stdout.flush()
    return

#
# --- Location Handler: รับพิกัดจาก LIFF แล้วเดิน flow ต่อทันที ---
@handler.add(MessageEvent, message=LocationMessageContent)
def handle_location_message(event):
    ensure_google_services()

    user_id = event.source.user_id
    lat = event.message.latitude
    lon = event.message.longitude
    addr = getattr(event.message, "address", "") or ""
    meta = _parse_meta_from_address(addr)  # คาดรูปแบบ "(txn=...|acc=...|ts=...)"
    txn  = meta.get("txn", "")
    acc  = meta.get("acc", "")
    tsms = meta.get("ts", "")

    # อ่านสถานะพนักงานก่อน
    employee_data, _ = get_employee_data(user_id)
    if employee_data == "__SHEETS_ERROR__":
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ระบบชีตช้าชั่วคราว ลองส่งตำแหน่งอีกครั้งภายหลังครับ")]
            )
        )
        return
    if not employee_data:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้ลงทะเบียน ไม่สามารถใช้งานได้ครับ")]
            )
        )
        return

    cur_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if len(employee_data) > EMPLOYEE_CURRENT_STATE_COL else "idle"
    cur_txn   = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if len(employee_data) > EMPLOYEE_CURRENT_TRANSACTION_ID_COL else ""

    # ต้องมี txn ตรงกัน (กันส่งพิกัดเก่า/ข้าม flow)
    if not txn or (cur_txn and txn != cur_txn):
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ไม่พบรหัสอ้างอิงธุรกรรมของตำแหน่ง (txn) กรุณาเริ่มใหม่อีกครั้งด้วยคำสั่งเดิม")]
            )
        )
        return

    # ตรวจความแม่นยำ/อายุพิกัดจากมือถือ
    try:
        acc_val = int(acc) if acc else 999999
    except Exception:
        acc_val = 999999
    if acc_val > MAX_GPS_ACCURACY_M:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text=f"ความแม่นยำ {acc_val}m สูงเกินกำหนด ({MAX_GPS_ACCURACY_M}m) กรุณาอยู่กลางแจ้ง/เปิด GPS แล้วลองใหม่")]
            )
        )
        return
    if tsms and not _is_recent_ts_ms(tsms, MAX_LOCATION_AGE_SEC):
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ตำแหน่งจากมือถือเก่าเกินไป กรุณากดส่งใหม่อีกครั้ง")]
            )
        )
        return

    # แยก flow: เช็คอิน vs ส่งงาน
    if cur_state in ("waiting_for_checkin_location",):
        # จับคู่ไซต์ด้วย checkin_radius
        loc_name, site_group, matched, dist_m = match_site_by_location(lat, lon)
        if not matched and SITE_NO_MATCH_POLICY.lower() == "reject":
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text=f"นอกเขตรัศมีเช็คอิน (ระยะ ~{int(dist_m or 0)} m) กรุณาเข้าใกล้จุดเช็คอินแล้วส่งใหม่")]
                )
            )
            return

        # upsert แถว CheckIns ให้ “1 checkin_id = 1 record”
        emp_name = employee_data[EMPLOYEE_NAME_COL] if len(employee_data) > EMPLOYEE_NAME_COL else ""
        row_idx = upsert_checkin_row_idempotent(txn, user_id, loc_name or f"{lat},{lon}", site_group or "", dist_m or 0, emp_name)
        # ไปสถานะรอรูป + แจ้งผู้ใช้
        update_employee_state(user_id, "waiting_for_checkin_images", txn)
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    V3TextMessage(
                        text=(f"ตำแหน่งรับแล้ว ✓\nสถานที่: {loc_name or 'พิกัดที่ส่งมา'} "
                              f"(ห่าง ~{int(dist_m or 0)} m)\nโปรดส่งรูปภาพหลักฐาน (ส่งทีละ 1 รูป สูงสุด 3 รูป)"),
                        quick_reply=QuickReply(items=[
                            QuickReplyItem(action=CameraAction(label="📸 ถ่ายภาพเช็คอิน")),
                            QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                            QuickReplyItem(action=MessageAction(label="✅ จบการเช็คอิน", text="จบการเช็คอิน")),
                            QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิกเช็คอิน")),
                        ]),
                    )
                ]
            )
        )
        return

    elif cur_state in ("waiting_for_submit_location",):
        # จับคู่ไซต์ด้วย submission_radius
        loc_name, site_group, matched, dist_m = match_site_by_location_for_submission(lat, lon)
        if not matched and SITE_NO_MATCH_POLICY.lower() == "reject":
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text=f"อยู่นอกเขตส่งงาน (ระยะ ~{int(dist_m or 0)} m) กรุณาเข้าใกล้จุดส่งงานแล้วส่งใหม่")]
                )
            )
            return

        # upsert แถว Submissions
        emp_name = employee_data[EMPLOYEE_NAME_COL] if len(employee_data) > EMPLOYEE_NAME_COL else ""
        _ = upsert_submission_row_idempotent(txn, user_id, loc_name or f"{lat},{lon}", site_group or "", dist_m or 0, emp_name)
        # ไปสถานะรอรูป + แจ้งผู้ใช้
        update_employee_state(user_id, "waiting_for_submit_images", txn)
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    V3TextMessage(
                        text=(f"ตำแหน่งส่งงานรับแล้ว ✓\nสถานที่: {loc_name or 'พิกัดที่ส่งมา'} "
                              f"(ห่าง ~{int(dist_m or 0)} m)\nโปรดส่งรูปงาน (ส่งทีละ 1 รูป สูงสุด 3 รูป)"),
                        quick_reply=QuickReply(items=[
                            QuickReplyItem(action=CameraAction(label="📸 ถ่ายรูปงาน")),
                            QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                            QuickReplyItem(action=MessageAction(label="✅ จบการส่งงาน", text="จบการส่งงาน")),
                            QuickReplyItem(action=MessageAction(label="ยกเลิก", text="ยกเลิกส่งงาน")),
                        ]),
                    )
                ]
            )
        )
        return

    # ถ้า state ไม่ตรงกับสอง flow ข้างบน
    line_bot_api.reply_message(
        ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[V3TextMessage(text="กรุณาเริ่มด้วย “เช็คอิน” หรือ “ส่งงาน” ก่อน แล้วค่อยส่งตำแหน่งครับ")]
        )
    )

@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message(event):
    ensure_google_services()

    user_id = event.source.user_id

    # อ่านสถานะพนักงานก่อน
    employee_data, _ = get_employee_data(user_id)
    if employee_data == "__SHEETS_ERROR__":
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ระบบชีตช้าชั่วคราว ลองส่งรูปอีกครั้งภายหลังครับ")]
            )
        )
        return
    if not employee_data:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้ลงทะเบียน ไม่สามารถใช้งานได้ครับ")]
            )
        )
        return

    state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if len(employee_data) > EMPLOYEE_CURRENT_STATE_COL else "idle"
    txn   = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if len(employee_data) > EMPLOYEE_CURRENT_TRANSACTION_ID_COL else ""

    # อนุญาตเฉพาะตอนรอรูป (เช็คอิน/ส่งงาน)
    if state not in ("waiting_for_checkin_images", "waiting_for_submit_images") or not txn:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้เข้าสู่ขั้นตอนส่งรูปครับ")]
            )
        )
        return

    # ดึง binary ของรูปจาก LINE
    try:
        content = blob_api.get_message_content(message_id=event.message.id)
        data = content.read()  # bytes
    except Exception as e:
        print(f"WARNING: cannot get image content: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ดึงรูปจาก LINE ไม่สำเร็จ ลองใหม่อีกครั้งครับ")]
            )
        )
        return

    # เตรียมรูป (ย่อ + เข้ารหัส JPEG) ตาม flow
    try:
        if state == "waiting_for_checkin_images":
            bio, ext, mime = prepare_image_for_checkin(data)
        else:
            bio, ext, mime = prepare_image_for_submission(data)
    except Exception as e:
        print(f"ERROR: prepare image failed: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="แปลงรูปภาพไม่สำเร็จ ลองถ่ายใหม่อีกครั้งครับ")]
            )
        )
        return

    # อัปขึ้น Google Drive (ต้อง authorize ก่อน)
    if drive_service is None:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้เชื่อมต่อ Google Drive กรุณาเปิด /authorize เพื่อเชื่อมต่อก่อนครับ")]
            )
        )
        return

    # ตั้งชื่อไฟล์บน Drive
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{state}_{txn}_{ts}.{ext}"

    media = MediaIoBaseUpload(bio, mimetype=mime, resumable=False)
    file_meta = {
        "name": fname,
        "parents": [GOOGLE_DRIVE_FOLDER_ID] if GOOGLE_DRIVE_FOLDER_ID else []
    }

    try:
        gfile = drive_service.files().create(body=file_meta, media_body=media, fields="id, webViewLink").execute()
        file_id = gfile.get("id")
        try:
            drive_service.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "reader"},
                fields="id"
            ).execute()
        except Exception:
            pass
        image_url = gfile.get("webViewLink") or f"https://drive.google.com/file/d/{file_id}/view"
    except Exception as e:
        print(f"ERROR: drive upload failed: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="อัปโหลดรูปขึ้น Drive ไม่สำเร็จ กรุณาลองใหม่อีกครั้งครับ")]
            )
        )
        return

    # เขียน URL ลงชีต
    try:
        if state == "waiting_for_checkin_images":
            idx, filled = _update_checkins_add_image_url(txn, image_url)
            remain = max(0, 3 - int(filled))
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        V3TextMessage(
                            text=f"บันทึกรูปเช็คอินแล้ว ✓ ส่งได้อีก {remain} รูป (ส่งทีละ 1 รูป สูงสุด 3 รูป)",
                            quick_reply=QuickReply(items=[
                                QuickReplyItem(action=CameraAction(label="📸 ถ่ายภาพเช็คอิน")),
                                QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                QuickReplyItem(action=MessageAction(label="✅ จบการเช็คอิน", text="จบการเช็คอิน")),
                            ])
                        )
                    ]
                )
            )
            return
        else:
            # Submission flow — ถ้าคุณมีการคำนวณ hash ให้แทนค่า image_hash_hex ได้
            image_hash_hex = ""
            idx, filled, dup_note = _update_submissions_add_image_url(txn, image_url, image_hash_hex)
            remain = max(0, 3 - int(filled))
            suffix = f"\n(พบซ้ำ: {dup_note})" if dup_note else ""
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        V3TextMessage(
                            text=f"บันทึกรูปงานแล้ว ✓ ส่งได้อีก {remain} รูป (ส่งทีละ 1 รูป สูงสุด 3 รูป){suffix}",
                            quick_reply=QuickReply(items=[
                                QuickReplyItem(action=CameraAction(label="📸 ถ่ายรูปงาน")),
                                QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                QuickReplyItem(action=MessageAction(label="✅ จบการส่งงาน", text="จบการส่งงาน")),
                            ])
                        )
                    ]
                )
            )
            return
    except Exception as e:
        print(f"ERROR: update sheet with image url failed: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="บันทึกรูปลงชีตไม่สำเร็จ กรุณาลองใหม่อีกครั้งครับ")]
            )
        )
        return

# --- FR-005 helpers: image duplicate detection for Submissions only ---
def _compute_image_ahash_from_jpeg_bytes(jpeg_bio: io.BytesIO) -> str:
    """
    Compute a simple 64-bit average-hash (aHash) from a JPEG byte stream.
    We decode with Pillow, convert to 8x8 grayscale, then threshold by mean.
    Returns hex string of 16 chars (64 bits).
    """
    try:
        jpeg_bio.seek(0)
        with Image.open(jpeg_bio) as im:
            im = im.convert("L").resize((8, 8), Image.LANCZOS)
            pixels = list(im.getdata())
            avg = sum(pixels) / 64.0
            bits = 0
            for i, p in enumerate(pixels):
                bits <<= 1
                if p >= avg:
                    bits |= 1
            return f"{bits:016x}"
    except Exception:
        return ""

def _find_duplicate_in_submissions(hash_hex: str, exclude_submit_id: str):
    """
    Scan Submissions sheet for any image hash (M..O columns) matching hash_hex.
    Returns (dup_submit_id, dup_row_index_1based, dup_image_slot_1to3) or (None, None, None) if not found.
    NOTE: This relies on hashes being stored for previous submissions.
    """
    if not hash_hex:
        return (None, None, None)
    rows = get_sheet_data(SUBMISSIONS_SHEET_NAME)
    if not rows or len(rows) < 2:
        return (None, None, None)
    # Header assumed in row1; data from row2
    for i, r in enumerate(rows[1:], start=2):
        if not r or len(r) == 0:
            continue
        submit_id = r[0] if len(r) > 0 else ""
        if not submit_id or submit_id == exclude_submit_id:
            continue
        # Hash columns: M (12), N (13), O (14) in 0-based indexing
        for slot in range(3):
            col = 12 + slot
            if len(r) > col and r[col]:
                if str(r[col]).strip().lower() == hash_hex.lower():
                    return (submit_id, i, slot + 1)
    return (None, None, None)

# --- Reusable Image Preparation Helpers (resize + encode JPEG) ---
def _prepare_image_bytes(image_bytes: bytes, max_dim: int, quality: int):
    """
    Decode image bytes, fix orientation, resize to max_dim (preserve aspect), convert to RGB,
    and encode to JPEG with given quality. Returns (io.BytesIO, ext, mime).
    """
    try:
        with Image.open(io.BytesIO(image_bytes)) as im:
            # Normalize orientation if EXIF present
            try:
                exif = im.getexif()
                orientation = exif.get(0x0112)
                if orientation == 3:
                    im = im.rotate(180, expand=True)
                elif orientation == 6:
                    im = im.rotate(270, expand=True)
                elif orientation == 8:
                    im = im.rotate(90, expand=True)
            except Exception:
                pass

            # Resize if larger than max_dim (preserve aspect ratio)
            w, h = im.size
            max_side = max(w, h)
            if max_side > max_dim:
                scale = max_dim / float(max_side)
                new_w = max(1, int(round(w * scale)))
                new_h = max(1, int(round(h * scale)))
                im = im.resize((new_w, new_h), Image.LANCZOS)

            # Convert to JPEG-friendly RGB (flatten alpha if needed)
            if im.mode in ("RGBA", "LA"):
                bg = Image.new("RGB", im.size, (255, 255, 255))
                alpha = im.split()[-1]
                bg.paste(im, mask=alpha)
                im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")

            out_bio = io.BytesIO()
            # Prefer per-flow quality; if not set, fallback to legacy IMAGE_JPEG_QUALITY
            q = int(quality) if quality else IMAGE_JPEG_QUALITY
            im.save(out_bio, format="JPEG", quality=q, optimize=True, progressive=True)
            out_bio.seek(0)

            return out_bio, "jpg", "image/jpeg"
    except Exception as e:
        # Re-raise for caller to handle
        raise e

def prepare_image_for_checkin(image_bytes: bytes):
    """Use per-flow quality for CHECK-IN images."""
    return _prepare_image_bytes(image_bytes, IMAGE_MAX_DIM, IMAGE_QUALITY_CHECKIN)

def prepare_image_for_submission(image_bytes: bytes):
    """Use per-flow quality for SUBMISSION images (FR-004)."""
    return _prepare_image_bytes(image_bytes, IMAGE_MAX_DIM, IMAGE_QUALITY_SUBMISSION)

# --- Dedicated Image Handler ---

def _reply_after_image(user_reply_token, filled_count: int, flow: str):
    """
    Send a reply after receiving an image.
    flow: "checkin" or "submission"
    """
    remaining = max(0, 3 - filled_count)
    if flow == "checkin":
        title_done = "✅ จบการเช็คอิน"
        ask_more  = "📸 ส่งภาพเช็คอินเพิ่ม"
        finish_text = "จบการเช็คอิน"
        base_text = f"บันทึกรูปเช็คอินแล้ว ✓ (ส่งทีละ 1 รูป สูงสุด 3 รูป)\nขณะนี้มี {filled_count}/3 รูป"
    else:
        title_done = "✅ จบการส่งงาน"
        ask_more  = "📸 ส่งรูปงานเพิ่ม"
        finish_text = "จบการส่งงาน"
        base_text = f"บันทึกรูปงานแล้ว ✓ (ส่งทีละ 1 รูป สูงสุด 3 รูป)\nขณะนี้มี {filled_count}/3 รูป"

    if remaining > 0:
        msg = V3TextMessage(
            text=base_text + f"\nยังส่งได้อีก {remaining} รูป",
            quick_reply=QuickReply(items=[
                QuickReplyItem(action=CameraAction(label=ask_more)),
                QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                QuickReplyItem(action=MessageAction(label=title_done, text=finish_text)),
            ])
        )
    else:
        msg = V3TextMessage(
            text=base_text + "\nครบ 3 รูปแล้ว",
            quick_reply=QuickReply(items=[
                QuickReplyItem(action=MessageAction(label=title_done, text=finish_text)),
            ])
        )
    line_bot_api.reply_message(
        ReplyMessageRequest(
            reply_token=user_reply_token,
            messages=[msg]
        )
    )


@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message(event):
    # Ensure services
    ensure_google_services()

    user_id = event.source.user_id
    employee_data, _ = get_employee_data(user_id)
    if employee_data == "__SHEETS_ERROR__":
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ตอนนี้เชื่อมต่อชีตไม่สำเร็จ กรุณาส่งรูปอีกครั้งภายหลังสักครู่")]
            )
        )
        print("DEBUG: Image path aborted due to Sheets error."); sys.stdout.flush()
        return
    current_state = employee_data[EMPLOYEE_CURRENT_STATE_COL] if employee_data else "idle"
    current_transaction_id = employee_data[EMPLOYEE_CURRENT_TRANSACTION_ID_COL] if employee_data else ""

    # Auto-timeout check before processing new image
    if _check_and_handle_timeout(user_id, reply_token=event.reply_token):
        return

    if not employee_data:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันนี้")]
            )
        )
        print("DEBUG: Replied: not registered (Image)."); sys.stdout.flush()
        return

    if current_state not in ("waiting_for_checkin_images", "waiting_for_submit_images") or not current_transaction_id:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาเริ่มขั้นตอนให้ถูกต้องก่อน (เช็คอินหรือส่งงาน) แล้วจึงส่งรูปครับ")]
            )
        )
        print("DEBUG: Replied: image received but not in waiting-for-images state."); sys.stdout.flush()
        return

    # --- Fetch image bytes from LINE ---
    try:
        message_id = event.message.id
        resp = blob_api.get_message_content(message_id)
        if hasattr(resp, "iter_content"):
            image_bytes = b"".join(chunk for chunk in resp.iter_content(chunk_size=1024))
        elif isinstance(resp, (bytes, bytearray)):
            image_bytes = bytes(resp)
        elif hasattr(resp, "read"):
            image_bytes = resp.read()
        else:
            image_bytes = bytes(resp)
    except Exception as e:
        print(f"ERROR: Unable to download image from LINE: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ดาวน์โหลดรูปจาก LINE ไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
            )
        )
        return

    # --- Decode & compress image ---
    is_submission_flow = (current_state == "waiting_for_submit_images")
    try:
        out_bio, ext, mime = (prepare_image_for_submission(image_bytes) if is_submission_flow else prepare_image_for_checkin(image_bytes))
    except Exception as e:
        print(f"ERROR: Pillow failed to process image: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ประมวลผลรูปภาพไม่สำเร็จ กรุณาลองส่งใหม่ (รองรับ JPEG/PNG/GIF)")]
            )
        )
        return

    file_prefix = "submission_image" if is_submission_flow else "checkin_image"
    file_name = f"{file_prefix}_{current_transaction_id}_{uuid.uuid4()}.{ext}"

    # --- Ensure Drive is authorized ---
    if drive_service is None:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้อนุญาตการอัปโหลด Google Drive กรุณาเปิดลิงก์ /authorize ในเบราว์เซอร์และยืนยันก่อนครับ")]
            )
        )
        print("DEBUG: Drive not authorized; abort image handling."); sys.stdout.flush()
        return

    # --- Upload to Drive ---
    try:
        media = MediaIoBaseUpload(out_bio, mimetype=mime, resumable=True)
        file_metadata = {"name": file_name, "parents": [GOOGLE_DRIVE_FOLDER_ID]} if GOOGLE_DRIVE_FOLDER_ID else {"name": file_name}
        created = _exec_with_timeout(
            lambda: drive_service.files().create(body=file_metadata, media_body=media, fields="id, webViewLink").execute(),
            DRIVE_EXECUTE_TIMEOUT_SEC,
            "Drive files.create"
        )
        file_id = created.get("id")
        uploaded_url = created.get("webViewLink")

        # Public permission (ไม่ critical ถ้าล้มเหลว)
        try:
            _exec_with_timeout(
                lambda: drive_service.permissions().create(fileId=file_id, body={"type": "anyone", "role": "reader"}).execute(),
                DRIVE_EXECUTE_TIMEOUT_SEC,
                "Drive permissions.create"
            )
        except Exception as e:
            print(f"WARNING: set public permission failed: {e}"); traceback.print_exc(); sys.stdout.flush()
    except Exception as e:
        print(f"ERROR: Drive upload failed: {e}")
        traceback.print_exc(); sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="อัปโหลดรูปไปยัง Drive ไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
            )
        )
        return

    # --- Write to Sheets (NO duplicate append) ---
    if not is_submission_flow:
        # CHECK-IN: ใช้ตัวช่วย idempotent
        try:
            idx, filled = _update_checkins_add_image_url(current_transaction_id, uploaded_url)
            if filled >= 3:
                msg = "บันทึกรูปครบ 3 รูปแล้ว ✅\nพิมพ์ 'จบ' เพื่อปิดการเช็คอิน"
            else:
                remain = 3 - filled
                msg = f"บันทึกรูปแล้ว ✅ เหลือส่งได้อีก {remain} รูป"
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        V3TextMessage(
                            text=msg,
                            quick_reply=QuickReply(items=[
                                QuickReplyItem(action=CameraAction(label="📸 ถ่ายภาพเช็คอิน")),
                                QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                QuickReplyItem(action=MessageAction(label="✅ จบการเช็คอิน", text="จบการเช็คอิน")),
                            ])
                        )
                    ]
                )
            )
        except Exception as e:
            print(f"ERROR: update CheckIns with image failed: {e}")
            traceback.print_exc(); sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="บันทึกรูปในชีตไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
                )
            )
            return
    else:
        # SUBMISSION: เก็บแฮช และตรวจซ้ำ (ไม่แจ้ง user ตาม requirement)
        try:
            try:
                image_hash_hex = _compute_image_ahash_from_jpeg_bytes(out_bio)
            except Exception:
                image_hash_hex = ""
            idx, filled, dup_note = _update_submissions_add_image_url(current_transaction_id, uploaded_url, image_hash_hex)
            if filled >= 3:
                msg = "บันทึกรูปงานครบ 3 รูปแล้ว ✅\nพิมพ์ 'จบ' เพื่อปิดการส่งงาน"
            else:
                remain = 3 - filled
                msg = f"บันทึกรูปงานแล้ว ✅ เหลือส่งได้อีก {remain} รูป"
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        V3TextMessage(
                            text=msg,
                            quick_reply=QuickReply(items=[
                                QuickReplyItem(action=CameraAction(label="📸 ถ่ายรูปงาน")),
                                QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                QuickReplyItem(action=MessageAction(label="✅ จบการส่งงาน", text="จบการส่งงาน")),
                            ])
                        )
                    ]
                )
            )
        except Exception as e:
            print(f"ERROR: update Submissions with image failed: {e}")
            traceback.print_exc(); sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="บันทึกรูปงานในชีตไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
                )
            )
            return
        
    # Auto-timeout check before processing new image
    if _check_and_handle_timeout(user_id, reply_token=event.reply_token):
        return

    if not employee_data:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาลงทะเบียนก่อนใช้งานฟังก์ชันนี้")]
            )
        )
        print("DEBUG: Replied: not registered (Image).")
        sys.stdout.flush()
        return

    if current_state not in ("waiting_for_checkin_images", "waiting_for_submit_images") or not current_transaction_id:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="กรุณาเริ่มขั้นตอนให้ถูกต้องก่อน (เช็คอินหรือส่งงาน) แล้วจึงส่งรูปครับ")]
            )
        )
        print("DEBUG: Replied: image received but not in waiting-for-images state."); sys.stdout.flush();
        return

    # --- Fetch image bytes from LINE ---
    try:
        message_id = event.message.id  # <-- valid only for ImageMessageContent
        resp = blob_api.get_message_content(message_id)
        if hasattr(resp, "iter_content"):
            image_bytes = b"".join(chunk for chunk in resp.iter_content(chunk_size=1024))
        elif isinstance(resp, (bytes, bytearray)):
            image_bytes = bytes(resp)
        elif hasattr(resp, "read"):
            image_bytes = resp.read()
        else:
            image_bytes = bytes(resp)
    except Exception as e:
        print(f"ERROR: Unable to download image from LINE: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ดาวน์โหลดรูปจาก LINE ไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
            )
        )
        return

    # --- Decode & compress image with Pillow (resize + JPEG re-encode) ---
    is_submission_flow = (current_state == "waiting_for_submit_images")
    try:
        out_bio, ext, mime = (prepare_image_for_submission(image_bytes) if is_submission_flow else prepare_image_for_checkin(image_bytes))
    except Exception as e:
        print(f"ERROR: Pillow failed to process image: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ประมวลผลรูปภาพไม่สำเร็จ กรุณาลองส่งใหม่ (รองรับ JPEG/PNG/GIF)")]
            )
        )
        return

    file_prefix = "submission_image" if is_submission_flow else "checkin_image"
    file_name = f"{file_prefix}_{current_transaction_id}_{uuid.uuid4()}.{ext}"

    # --- Ensure Drive is authorized ---
    if drive_service is None:
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="ยังไม่ได้เชื่อมต่อ Google Drive กรุณาเปิด /authorize ในเบราว์เซอร์ก่อนครับ")]
            )
        )
        print("ERROR: Drive service is None. Ask user to authorize via /authorize.")
        sys.stdout.flush()
        return

    # --- Upload to Drive ---
    try:
        if not GOOGLE_DRIVE_FOLDER_ID:
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="ยังไม่ได้ตั้งค่าโฟลเดอร์ปลายทางใน Google Drive (GOOGLE_DRIVE_FOLDER_ID). กรุณาตั้งค่าใน .env แล้วเริ่มใหม่ครับ")]
                )
            )
            print("ERROR: GOOGLE_DRIVE_FOLDER_ID is not set; cannot upload.")
            sys.stdout.flush()
            return

        media = MediaIoBaseUpload(out_bio, mimetype=mime, resumable=False)
        file_metadata = {'name': file_name, 'parents': [GOOGLE_DRIVE_FOLDER_ID]}

        create_req = drive_service.files().create(
        body=file_metadata, media_body=media, fields='id,webViewLink', supportsAllDrives=True
        )
        uploaded = _exec_with_timeout(lambda: create_req.execute(num_retries=3),
                              DRIVE_EXECUTE_TIMEOUT_SEC,
                              "Drive files.create")
        image_url = uploaded.get('webViewLink')
        print(f"DEBUG: Image uploaded to Drive: {image_url}")
        sys.stdout.flush()

        # Try to set public permission (ignore failure)
        try:
            perm_req = drive_service.permissions().create(
                fileId=uploaded['id'], body={'type': 'anyone', 'role': 'reader'}, supportsAllDrives=True
            )
            _ = _exec_with_timeout(lambda: perm_req.execute(num_retries=3),
                                DRIVE_EXECUTE_TIMEOUT_SEC,
                                "Drive permissions.create")
            print("DEBUG: Image permission set to public.")
        except Exception as pe:
            print(f"WARNING: set public permission failed: {pe}")
            traceback.print_exc()
            sys.stdout.flush()

    except Exception as e:
        print(f"ERROR: Upload to Drive failed: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text="อัปโหลดรูปไป Google Drive ไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
            )
        )
        return

    # --- Update Submissions sheet (FR-005: store hash and duplicate reference) ---
    if is_submission_flow:
        try:
            # 1) Compute aHash from the processed JPEG bytes
            img_hash = _compute_image_ahash_from_jpeg_bytes(out_bio)

            # 2) Lookup duplicate in previous submissions (if any)
            dup_submit_id, dup_row_idx, dup_slot = _find_duplicate_in_submissions(img_hash, current_transaction_id)

            # 3) Load current submission row and fill next empty slot(s)
            sub_row, sub_idx = _find_submissions_row_by_id(current_transaction_id)
            if not sub_idx:
                # If row missing due to timing, upsert minimally then re-read
                upsert_submission_row_idempotent(current_transaction_id, user_id, "", "", 0)
                sub_row, sub_idx = _find_submissions_row_by_id(current_transaction_id)

            # Ensure we can write up to R (index 17)
            if not sub_row:
                sub_row = []
            _ensure_row_len(sub_row, 18)  # A..R

            # Find next empty image slot (F..H => idx 5..7)
            target_img_col = None
            for j in range(5, 8):
                if j >= len(sub_row) or not sub_row[j]:
                    target_img_col = j
                    break
            if target_img_col is None:
                # Already 3 images; keep last_updated_at and reply, do not overwrite
                sub_row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                update_sheet_data(SUBMISSIONS_SHEET_NAME, f"{SUBMISSIONS_SHEET_NAME}!A{sub_idx}:R{sub_idx}", sub_row)
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="ได้รับรูปครบ 3 รูปแล้ว ✅ หากต้องการจบ พิมพ์ 'จบการส่งงาน'")]
                    )
                )
                return

            # Map image slot F/G/H to hash columns M/N/O and duplicate refs P/Q/R
            img_slot = (target_img_col - 4)  # 1..3
            hash_col = 11 + img_slot         # M..O => 12..14 (0-based index)
            dup_col  = 14 + img_slot         # P..R => 15..17 (0-based index)

            # Write URL
            sub_row[target_img_col] = image_url
            # Write hash
            _ensure_row_len(sub_row, max(18, hash_col + 1))
            sub_row[hash_col] = img_hash or ""
            # Write duplicate reference if found (format: submitId#slot@row)
            if dup_submit_id:
                _ensure_row_len(sub_row, max(18, dup_col + 1))
                sub_row[dup_col] = f"{dup_submit_id}#{dup_slot}@row{sub_row if isinstance(dup_row_idx, int) else ''}"

            # Update timestamps/status
            sub_row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if len(sub_row) > 9 and not sub_row[9]:
                sub_row[9] = "in_progress"

            update_sheet_data(SUBMISSIONS_SHEET_NAME, f"{SUBMISSIONS_SHEET_NAME}!A{sub_idx}:R{sub_idx}", sub_row)

            # Gentle guidance (no duplicate disclosure to user)
            remaining = 0
            for j in range(5, 8):
                if j < len(sub_row) and sub_row[j]:
                    remaining += 1
            left = max(0, 3 - remaining)
            tip = "ส่งรูปเพิ่มได้อีก {} รูป หรือพิมพ์ 'จบการส่งงาน'".format(left) if left else "พิมพ์ 'จบการส่งงาน' เพื่อปิดงาน"
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text=f"บันทึกรูปงานเรียบร้อย ✓ {tip}")]
                )
            )
            return

        except Exception as e:
            print(f"ERROR: update Submissions (hash/dup) failed: {e}")
            traceback.print_exc(); sys.stdout.flush()
            # Fallback to simple URL append only (no hash/dup)
            try:
                sub_row, sub_idx = _find_submissions_row_by_id(current_transaction_id)
                if not sub_idx:
                    upsert_submission_row_idempotent(current_transaction_id, user_id, "", "", 0)
                    sub_row, sub_idx = _find_submissions_row_by_id(current_transaction_id)
                if not sub_row:
                    sub_row = []
                _ensure_row_len(sub_row, 12)
                # next empty F..H
                tgt = None
                for j in range(5, 8):
                    if j >= len(sub_row) or not sub_row[j]:
                        tgt = j; break
                if tgt is not None:
                    sub_row[tgt] = image_url
                sub_row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if len(sub_row) > 9 and not sub_row[9]:
                    sub_row[9] = "in_progress"
                update_sheet_data(SUBMISSIONS_SHEET_NAME, f"{SUBMISSIONS_SHEET_NAME}!A{sub_idx}:L{sub_idx}", sub_row)
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปงานเรียบร้อย ✓")]
                    )
                )
                return
            except Exception as e2:
                print(f"ERROR: Submissions simple fallback failed: {e2}")
                traceback.print_exc(); sys.stdout.flush()
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปงานไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
                    )
                )
                return

    # --- Update CheckIns sheet (fill image_url_1..3) ---
    if not is_submission_flow:
        try:
            # Prefer LINE's imageSet.index if provided (1-based). Fallback to "next empty slot".
            image_set = getattr(event.message, "image_set", None) or getattr(event.message, "imageSet", None)
            target_slot_idx = None  # 0-based sheet column index j (5..7 → F..H)
            if image_set and hasattr(image_set, "index"):
                try:
                    idx_val = int(image_set.index)
                    if idx_val in (1, 2, 3):
                        target_slot_idx = 4 + idx_val  # 1→5(F), 2→6(G), 3→7(H)
                except Exception:
                    target_slot_idx = None  # ignore parsing errors
            # Use a per-transaction lock to prevent conflicting concurrent updates
            lock = _txn_locks[current_transaction_id]
            with lock:
                checkins_data = get_sheet_data("CheckIns")
                row_idx_1based = None
                row = None
                for i, r in enumerate(checkins_data):
                    if r and len(r) > 0 and r[0] == current_transaction_id:
                        row = r
                        row_idx_1based = i + 1
                        break
                if row is None or row_idx_1based is None:
                    line_bot_api.reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[V3TextMessage(text="ไม่พบข้อมูลเช็คอินล่าสุดในชีต กรุณาเริ่มเช็คอินใหม่อีกครั้ง")]
                        )
                    )
                    print("ERROR: CheckIns row for current transaction not found.")
                    sys.stdout.flush()
                    return
                _ensure_row_len(row, 12)  # A..L
                if target_slot_idx is not None:
                    j = target_slot_idx
                    if j < len(row) and row[j]:
                        j = None
                        for cand in range(5, 8):
                            if not (cand < len(row) and row[cand]):
                                j = cand
                                break
                        if j is None:
                            pass
                else:
                    j = None
                    for cand in range(5, 8):
                        if not (cand < len(row) and row[cand]):
                            j = cand
                            break
                if j is not None:
                    while len(row) <= j:
                        row.append("")
                    row[j] = image_url
                row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # I
                row[9] = "in_progress"  # J
                if len(row) > 10:
                    row[10] = ""  # K: warning_sent
                images_now = 0
                for cand in range(5, 8):
                    if cand < len(row) and row[cand]:
                        images_now += 1
                update_sheet_data("CheckIns", f"CheckIns!A{row_idx_1based}:L{row_idx_1based}", row)
            if images_now >= 3:
                _finalize_checkin(user_id, current_transaction_id, "done")
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปภาพหลักฐานครบถ้วนแล้ว ✅ ระบบปิดเช็คอินให้เรียบร้อย")]
                    )
                )
                print("DEBUG: Replied: complete 3 images → finalize done.")
            else:
                remaining = 3 - images_now
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[
                            V3TextMessage(
                                text=f"บันทึกรูปภาพเรียบร้อยแล้ว ✅\nสามารถส่งภาพได้เพิ่มอีก {remaining} รูป (ส่งครั้งละ 1 รูป)\nหากต้องการจบการเช็คอินให้กด “จบการเช็คอิน”",
                                quick_reply=QuickReply(items=[
                                    QuickReplyItem(action=CameraAction(label="📸 ส่งภาพเพิ่ม")),
                                    QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                    QuickReplyItem(action=MessageAction(label="✅ จบการเช็คอิน", text="จบการเช็คอิน")),
                                ]),
                            )
                        ]
                    )
                )
                print(f"DEBUG: Replied: image saved; waiting for more images (remaining={remaining}).")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR: Updating CheckIns failed: {e}")
            traceback.print_exc()
            sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="บันทึกลิงก์รูปลงชีตไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
                )
            )
            return
    else:
        # Submission flow: update Submissions sheet
        try:
            lock = _txn_locks[current_transaction_id]
            with lock:
                subs_data = get_sheet_data(SUBMISSIONS_SHEET_NAME)
                row_idx_1based = None
                row = None
                for i, r in enumerate(subs_data or []):
                    if r and len(r) > 0 and r[0] == current_transaction_id:
                        row = r
                        row_idx_1based = i + 1
                        break
                if row is None or row_idx_1based is None:
                    line_bot_api.reply_message(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[V3TextMessage(text="ไม่พบข้อมูลการส่งงานล่าสุดในชีต กรุณาเริ่มใหม่อีกครั้ง")]
                        )
                    )
                    print("ERROR: Submissions row for current transaction not found.")
                    sys.stdout.flush()
                    return

                _ensure_row_len(row, 12)  # A..L (0..11)

                # honor image set index if provided, else first empty slot F..H (5..7)
                image_set = getattr(event.message, "image_set", None) or getattr(event.message, "imageSet", None)
                target_slot_idx = None
                if image_set and hasattr(image_set, "index"):
                    try:
                        idx_val = int(image_set.index)
                        if idx_val in (1, 2, 3):
                            target_slot_idx = 4 + idx_val  # map to F..H (5..7)
                    except Exception:
                        target_slot_idx = None

                if target_slot_idx is not None:
                    j = target_slot_idx
                    if j < len(row) and row[j]:
                        j = None
                        for cand in range(5, 8):
                            if not (cand < len(row) and row[cand]):
                                j = cand
                                break
                else:
                    j = None
                    for cand in range(5, 8):
                        if not (cand < len(row) and row[cand]):
                            j = cand
                            break

                if j is not None:
                    while len(row) <= j:
                        row.append("")
                    row[j] = image_url

                # mark progress + refresh timestamp
                row[8] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # I: last_updated_at
                row[9] = "in_progress"  # J: status
                update_sheet_data(SUBMISSIONS_SHEET_NAME, f"{SUBMISSIONS_SHEET_NAME}!A{row_idx_1based}:L{row_idx_1based}", row)

                # count images now
                images_now = 0
                for cand in range(5, 8):  # F..H
                    if cand < len(row) and row[cand]:
                        images_now += 1

            # outside lock: finalize or ask for more
            if images_now >= 3:
                _finalize_submission(user_id, current_transaction_id, "done")
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[V3TextMessage(text="บันทึกรูปงานครบถ้วนแล้ว ✅ ระบบปิดการส่งงานให้เรียบร้อย")]
                    )
                )
                print("DEBUG: Submission complete with 3 images → finalize.")
                sys.stdout.flush()
            else:
                remaining = 3 - images_now
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[
                            V3TextMessage(
                                text=f"บันทึกรูปงานแล้ว ✅ ยังสามารถส่งรูปได้อีก {remaining} รูป (ครั้งละ 1 รูป)\nหากต้องการจบให้กด “จบการส่งงาน”",
                                quick_reply=QuickReply(items=[
                                    QuickReplyItem(action=CameraAction(label="📸 ส่งรูปเพิ่ม")),
                                    QuickReplyItem(action=CameraRollAction(label="🖼 เลือกจากคลังภาพ")),
                                    QuickReplyItem(action=MessageAction(label="✅ จบการส่งงาน", text="จบการส่งงาน")),
                                ]),
                            )
                        ]
                    )
                )
                print(f"DEBUG: Submission image saved; waiting for more (remaining={remaining}).")
                sys.stdout.flush()
            return
        except Exception as e:
            print(f"ERROR: Updating Submissions failed: {e}")
            traceback.print_exc()
            sys.stdout.flush()
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[V3TextMessage(text="บันทึกรูปส่งงานลงชีตไม่สำเร็จ กรุณาลองใหม่อีกครั้ง")]
                )
            )
            return
# --- END OF FLASK ROUTES AND HANDLERS ---


# --- Startup: Run Flask app and scheduler if this is the main module ---

if __name__ == "__main__":
    import atexit

    # Ensure Google services are ready (Sheets/Drive)
    try:
        ensure_google_services()
    except Exception as e:
        print(f"ERROR: ensure_google_services failed at startup: {e}")
        import traceback, sys
        traceback.print_exc(); sys.stdout.flush()

    # Start Background Scheduler for timeout scanning
    try:
        if scheduler is None:
            scheduler = BackgroundScheduler(timezone=APP_TIMEZONE, job_defaults={"max_instances": 1, "coalesce": True})
            scheduler.add_job(_scan_and_timeout_overdue_checkins,
                              trigger="interval",
                              seconds=SCHEDULER_INTERVAL_SECONDS,
                              id="scan_timeout_jobs",
                              max_instances=1,
                              coalesce=True,
                              replace_existing=True)
            scheduler.start()
            print(f"DEBUG: Scheduler started (interval={SCHEDULER_INTERVAL_SECONDS}s, tz={APP_TIMEZONE})")
    except Exception as e:
        print(f"ERROR: Failed to start scheduler: {e}")
        import traceback, sys
        traceback.print_exc(); sys.stdout.flush()

    # Ensure scheduler shuts down cleanly on exit
    def _shutdown_scheduler():
        global scheduler
        try:
            if scheduler:
                scheduler.shutdown(wait=False)
                print("DEBUG: Scheduler shut down")
        except Exception:
            pass
    atexit.register(_shutdown_scheduler)

    # Start Flask development server
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    debug_flag = os.getenv("FLASK_DEBUG", "0") == "1"
    print(f"DEBUG: Starting Flask on {host}:{port} (debug={debug_flag})")
    app.run(host=host, port=port, debug=debug_flag, use_reloader=False)