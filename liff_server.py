# liff_server.py
# This file serves the LIFF app and handles its specific logic.

from flask import Flask, request, abort, render_template_string
import os
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# --- LINE LIFF Configuration ---
LIFF_ID = os.getenv('LIFF_ID') # You need to set this in your .env file

if not LIFF_ID:
    raise ValueError("LIFF_ID not set in .env for liff_server.py")

# --- LIFF App Serving Route ---
@app.route('/liff_location_picker')
def serve_liff_location_picker():
    print("DEBUG: Serving liff_location_picker.html")
    sys.stdout.flush()
    try:
        with open('liff_location_picker.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        # Inject LIFF_ID into the HTML content
        html_content = html_content.replace("YOUR_LIFF_ID", LIFF_ID)
        print("DEBUG: liff_location_picker.html served successfully.")
        sys.stdout.flush()
        return render_template_string(html_content)
    except FileNotFoundError:
        print("ERROR: liff_location_picker.html not found.")
        sys.stdout.flush()
        abort(404)
    except Exception as e:
        print(f"ERROR: Error serving liff_location_picker.html: {e}")
        sys.stdout.flush()
        abort(500)

# --- Main execution ---
if __name__ == '__main__':
    print("Starting LIFF server...")
    sys.stdout.flush()
    app.run(port=8001) # Run on a different port than main.py (e.g., 8001)
