# -*- coding: utf-8 -*-
import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
# Removed unused telegram.* imports as we are using telebot consistently
# from telegram import Update
# from telegram.ext import Updater, CommandHandler, CallbackContext
import psutil
import sqlite3
import json 
import logging 
import signal 
import threading
import re 
import sys 
import atexit
import requests 

# Webhook के लिए Flask को शामिल करें और Polling Threads को हटा दें
from flask import Flask, request 
# 'Thread' अब केवल 'run_script' आदि के लिए इस्तेमाल होगा

# --- Configuration ---
# TOKEN अब os.environ.get() से आएगा, यह Render पर सुरक्षित है
TOKEN = os.environ.get('TOKEN') 
# बाकी Configuration आपके पुराने कोड से
OWNER_ID = 6706754806 # Replace with your Owner ID
ADMIN_ID = 6706754806 # Replace with your Admin ID (can be same as Owner)
YOUR_USERNAME = '@pfp_kahi_nhi_milega' # Replace with your Telegram username (without the @)
UPDATE_CHANNEL = 'https://t.me/pfp_kahi_nhi_milega' # Replace with your update channel link

# Folder setup - using absolute paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__)) # Get script's directory
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf') # Assuming this name is intentional
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')

# File upload limits
FREE_USER_LIMIT = 5
SUBSCRIBED_USER_LIMIT = 20 
ADMIN_LIMIT = 999       
OWNER_LIMIT = float('inf') 

# Check for Token and exit if not set (Crucial for Render Deployment)
if not TOKEN:
    # Use logger for critical errors
    logging.error("❌ ERROR: Telegram TOKEN environment variable is not set. Exiting.")
    sys.exit(1)

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {} # Stores info about running scripts {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
active_users = set() # Set of all user IDs that have interacted with the bot
admin_ids = {ADMIN_ID, OWNER_ID} # Set of admin IDs
bot_locked = False

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
# ... (आपके सभी Buttons Layouts यहाँ रहेंगे)
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"], 
    ["📞 Contact Owner"]
]
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"], 
    ["👑 Admin Panel", "📞 Contact Owner"]
]


# --- Database Setup and Loading ---
# ... (init_db, load_data, save_user_file, remove_user_file_db, add_active_user, save_subscription, remove_subscription_db, आदि सभी Functions आपके पुराने कोड से यहाँ रहेंगे)

# Database Setup and Loading at startup
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False) 
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''') 
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
             c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall()) 

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

init_db()
load_data()

# --- Helper Functions, Run Script Functions, Database Operations, etc. ---
# ... (get_user_folder, get_user_file_limit, is_bot_running, kill_process_tree, run_script, run_js_script, save_user_file, remove_user_file_db, add_active_user, save_subscription, remove_subscription_db, आदि सभी Functions आपके पुराने कोड से यहाँ रहेंगे)

def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name):
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" 
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False

def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A') 

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid: 
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)


def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install Node package `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ Error: 'npm' not found. Ensure Node.js/npm are installed and in PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ Error installing Node package `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2 
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"❌ Error in script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix the script.", parse_mode='Markdown')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, 
                'script_owner_id': script_owner_id, 
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]


def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix script or install manually.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, 
                'script_owner_id': script_owner_id, 
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ Error: 'node' not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

TELEGRAM_MODULES = {
    'telebot': 'pyTelegramBotAPI',
    # ... (बाकी सभी Modules आपके पुराने कोड से)
    # Main Bot Frameworks
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon', 
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',
    'mtproto': 'telegram-mtproto', 
    'tl': 'telethon',  
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai', 
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow', 
    'cv2': 'opencv-python', 
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'psutil': 'psutil',
    # Core modules that should be None
    'asyncio': None, 
    'json': None,    
    'datetime': None,
    'os': None,      
    'sys': None,     
    're': None,      
    'time': None,    
    'math': None,    
    'random': None,  
    'logging': None, 
    'threading': None,
    'subprocess':None,
    'zipfile':None,  
    'tempfile':None, 
    'shutil':None,   
    'sqlite3':None,  
    'atexit': None   
}

DB_LOCK = threading.Lock() 

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files: user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving file for user {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing file for {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def add_active_user(user_id):
    active_users.add(user_id) 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding active user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding active user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions: del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id)
            logger.info(f"Added admin {admin_id} to DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()
        
def remove_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
            conn.commit()
            if admin_id in admin_ids: admin_ids.remove(admin_id)
            logger.info(f"Removed admin {admin_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()


# --- Logic Functions (called by commands and text handlers) ---
# ... (आपके सभी _logic_* Functions यहाँ रहेंगे)
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name or "User"
    add_active_user(user_id) # Log the user as active

    # Check for expired subscriptions and clean up if needed
    if user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date <= datetime.now():
            logger.info(f"Subscription expired for user {user_id}. Removing from active subs.")
            # Do NOT remove files, but degrade their limit
            remove_subscription_db(user_id)
    
    # Determine user status and limits
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    
    expiry_info = ""
    user_status = "🆓 Free User"
    if user_id == OWNER_ID:
        user_status = "👑 Owner"
    elif user_id in admin_ids:
        user_status = "🛡️ Admin"
    elif user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        expiry_date = user_subscriptions[user_id].get('expiry')
        days_left = (expiry_date - datetime.now()).days if expiry_date else 0
        user_status = "⭐ Premium"
        expiry_info = f"\n⏳ Subscription expires in: {days_left} days"

    welcome_msg_text = (f"👋 Namaste **{user_name}**!\n\n"
                        f"🆔 ID: `{user_id}`\n"
                        f"🔰 Status: {user_status}{expiry_info}\n"
                        f"📁 Files: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f" Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    
    # Try to send welcome message
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    photo_file_id = os.environ.get('WELCOME_PHOTO_ID') # Assuming a way to set photo ID
    try:
        if photo_file_id:
            bot.send_photo(chat_id, photo_file_id)
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown') # Fallback without photo
        except Exception as fallback_e:
            logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message_or_call):
    chat_id = message_or_call.chat.id if isinstance(message_or_call, types.Message) else message_or_call.message.chat.id
    bot.send_message(chat_id, f"📢 Our Official Updates Channel: {UPDATE_CHANNEL}")

def _logic_upload_file(message):
    user_id = message.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete some files or get a subscription.")
        return
        
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message_or_call):
    user_id = message_or_call.from_user.id
    chat_id = message_or_call.chat.id if isinstance(message_or_call, types.Message) else message_or_call.message.chat.id
    
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.send_message(chat_id, "📂 Your files:\n\n(No files uploaded)")
        return
        
    file_status_messages = []
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        file_status_messages.append(f"• `{file_name}` ({file_type}) - **{status_icon}**")
        
    response_text = "📂 Your files:\n\n" + "\n".join(file_status_messages)
    
    # Create inline markup to control files
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    
    bot.send_message(chat_id, response_text, reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message_or_call):
    chat_id = message_or_call.chat.id if isinstance(message_or_call, types.Message) else message_or_call.message.chat.id
    
    cpu_percent = psutil.cpu_percent(interval=1)
    ram_usage = psutil.virtual_memory()
    total_running_scripts = len(bot_scripts)
    
    # Format running scripts list
    running_list = []
    if total_running_scripts > 0:
        for script_key, info in bot_scripts.items():
            run_time = datetime.now() - info['start_time']
            hours, remainder = divmod(run_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"{hours:02}:{minutes:02}:{seconds:02}"
            running_list.append(f"• `{info['file_name']}` (User: {info['script_owner_id']}) - Up {uptime_str}")
        
    scripts_info = "Running Scripts:\n" + "\n".join(running_list) if running_list else "Running Scripts: None"

    message_text = (f"⚡ **Bot Speed & System Status**\n\n"
                    f"CPU Usage: `{cpu_percent:.2f}%`\n"
                    f"RAM Usage: `{ram_usage.percent:.2f}%` ({ram_usage.used / (1024**3):.2f}GB / {ram_usage.total / (1024**3):.2f}GB)\n"
                    f"Total Running Scripts: `{total_running_scripts}`\n\n"
                    f"{scripts_info}")
                    
    bot.send_message(chat_id, message_text, parse_mode='Markdown')

def _logic_contact_owner(message):
    bot.reply_to(message, f"📞 Owner Contact:\n\nTelegram Username: {YOUR_USERNAME}")

def _logic_statistics(message_or_call):
    chat_id = message_or_call.chat.id if isinstance(message_or_call, types.Message) else message_or_call.message.chat.id
    
    total_users = len(active_users)
    total_subscriptions = len(user_subscriptions)
    total_admins = len(admin_ids)
    total_running_scripts = len(bot_scripts)
    
    # Calculate total files uploaded
    total_files = sum(len(files) for files in user_files.values())
    
    stats_text = (f"📊 **Bot Statistics**\n\n"
                  f"👤 Total Users: `{total_users}`\n"
                  f"⭐ Total Subscriptions: `{total_subscriptions}`\n"
                  f"🛡️ Total Admins: `{total_admins}`\n"
                  f"📁 Total Files Uploaded: `{total_files}`\n"
                  f"🟢 Currently Running Scripts: `{total_running_scripts}`")
                  
    bot.send_message(chat_id, stats_text, parse_mode='Markdown')
    
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Add Subscription", callback_data='add_sub_init'))
    markup.add(types.InlineKeyboardButton("➖ Remove Subscription", callback_data='remove_sub_init'))
    markup.add(types.InlineKeyboardButton("📜 List Active Subs", callback_data='list_subs'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    bot.reply_to(message, "💳 **Subscription Management Panel**", reply_markup=markup, parse_mode='Markdown')

def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
        
    msg = bot.send_message(message.chat.id, "📢 Send the message (text, photo, video, etc.) you want to broadcast to all users, or type /cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    global bot_locked
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
        
    bot_locked = not bot_locked
    status = "🔒 LOCKED" if bot_locked else "🔓 UNLOCKED"
    bot.reply_to(message, f"🤖 Bot is now **{status}**.", parse_mode='Markdown')

def _logic_run_all_scripts(message_or_call):
    admin_user_id = None
    reply_func = None
    admin_message_obj_for_script_runner = None
    
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return 
        
    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")
    
    started_count = 0
    attempted_users = 0
    skipped_files = 0
    error_files_details = [] 
    
    all_user_files_snapshot = dict(user_files)
    
    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user:
            continue
        
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)
        
        for file_name, file_type in files_for_user:
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            skipped_files += 1
                            error_files_details.append(f"Skipped '{file_name}' (User: {target_user_id}) - Unknown type '{file_type}'")
                    except Exception as e:
                        error_files_details.append(f"Error starting '{file_name}' (User: {target_user_id}): {e}")
                        logger.error(f"Error starting script for user {target_user_id}, file {file_name}: {e}")
                else:
                    error_files_details.append(f"Missing file '{file_name}' (User: {target_user_id}) - Removed from DB.")
                    remove_user_file_db(target_user_id, file_name)
                    skipped_files += 1
            else:
                skipped_files += 1
                
    final_message = (f"✅ Run All Scripts Process Complete!\n\n"
                     f"👤 Users Attempted: `{attempted_users}`\n"
                     f"🟢 Scripts Started: `{started_count}`\n"
                     f"🟡 Scripts Skipped (Already running/Missing/Unknown Type): `{skipped_files}`\n"
                     f"❌ Errors Found: `{len(error_files_details)}`")
                     
    if error_files_details:
        final_message += "\n\n**Error/Missing Details:**\n" + "\n".join(error_files_details[:5])
        if len(error_files_details) > 5:
             final_message += f"\n...and {len(error_files_details) - 5} more."
             
    reply_func(final_message, parse_mode='Markdown')

def _logic_admin_panel(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner permissions required to access the full admin panel.")
        return
        
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Add Admin", callback_data='add_admin_init'))
    markup.add(types.InlineKeyboardButton("➖ Remove Admin", callback_data='remove_admin_init'))
    markup.add(types.InlineKeyboardButton("📜 List Admins", callback_data='list_admins'))
    markup.add(types.InlineKeyboardButton("❌ Stop ALL Scripts", callback_data='stop_all_scripts'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    bot.reply_to(message, "👑 **Owner Admin Panel**", reply_markup=markup, parse_mode='Markdown')

# --- Handler Mappings ---
BUTTON_TEXT_TO_LOGIC = {
    "/start": _logic_send_welcome, # Added /start for consistency
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics, 
    "💳 Subscriptions": _logic_subscriptions_panel,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot, 
    "🟢 Running All Code": _logic_run_all_scripts, 
    "👑 Admin Panel": _logic_admin_panel,
}

# --- Message Handlers (Commands and Text) ---
@bot.message_handler(commands=['start', 'help'])
def command_start_help(message):
    _logic_send_welcome(message)
    
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message):
    _logic_upload_file(message)

@bot.message_handler(commands=['checkfiles'])
def command_check_files(message):
    _logic_check_files(message)

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message):
    _logic_updates_channel(message)

@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message):
    _logic_bot_speed(message)

@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message):
    _logic_contact_owner(message)

@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message):
    _logic_subscriptions_panel(message)

@bot.message_handler(commands=['statistics', 'status']) 
def command_statistics(message):
    _logic_statistics(message)

@bot.message_handler(commands=['broadcast'])
def command_broadcast(message):
    _logic_broadcast_init(message)

@bot.message_handler(commands=['lockbot'])
def command_lock_bot(message):
    _logic_toggle_lock_bot(message)

@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message):
    _logic_admin_panel(message)

@bot.message_handler(commands=['runningallcode']) 
def command_run_all_code(message):
    _logic_run_all_scripts(message)

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    # Check bot lock status first
    if bot_locked and message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot is currently locked by admin.")
        return

    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func:
        logic_func(message)
    else:
        logger.warning(f"Button text '{message.text}' matched but no logic func.")
# --- End Message Handlers ---

# --- Document Handler (for file upload) ---
@bot.message_handler(content_types=['document'])
def handle_document(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot is currently locked by admin. Cannot upload.")
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete some files or get a subscription.")
        return

    # Rest of the file handling logic (handle_zip_file, handle_js_file, handle_py_file etc.)
    # ... (आपके पुराने handle_document और उसके सहायक functions यहाँ रहेंगे)
    
    if not message.document.file_name:
        bot.reply_to(message, "⚠️ File must have a name.")
        return

    file_name = message.document.file_name
    file_id = message.document.file_id
    
    # Check for invalid file names
    if any(c in file_name for c in ['/', '\\', '..']):
        bot.reply_to(message, "⚠️ Invalid file name. Avoid special characters like '/', '\', '..'")
        return

    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "⚠️ Only Python (`.py`), Javascript (`.js`), and ZIP (`.zip`) files are supported.")
        return
        
    # Prevent overwriting main bot file
    if file_name.lower() == 'add.py' or file_name.lower() == 'main.py':
        bot.reply_to(message, "⚠️ Cannot upload a file named 'ADD.py' or 'main.py'. Rename your file.")
        return

    if file_ext == '.zip' and get_user_file_count(user_id) > 0:
        bot.reply_to(message, "⚠️ Cannot upload a ZIP file if you already have single files. Please delete existing files first.")
        return

    # Check if a file with the same name already exists
    if file_name in [f[0] for f in user_files.get(user_id, [])]:
        bot.reply_to(message, f"⚠️ A file named `{file_name}` already exists. Please rename and re-upload, or delete the old one.", parse_mode='Markdown')
        return

    bot.reply_to(message, f"📥 Downloading file: `{file_name}`...", parse_mode='Markdown')

    try:
        # Get the file path on Telegram servers
        file_info = bot.get_file(file_id)
        downloaded_file_content = bot.download_file(file_info.file_path)
        
        user_folder = get_user_folder(user_id)

        if file_ext == '.zip':
            # Handle ZIP file extraction and dependencies
            handle_zip_file(downloaded_file_content, file_name, message)
        else:
            # Handle single .py or .js file
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f:
                f.write(downloaded_file_content)
            logger.info(f"Saved single file to {file_path}")

            if file_ext == '.js':
                # Pass user_id as script_owner_id
                handle_js_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.py':
                # Pass user_id as script_owner_id
                handle_py_file(file_path, user_id, user_folder, file_name, message)
                
    except telebot.apihelper.ApiTelegramException as e:
        logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
        if "file is too big" in str(e).lower():
            bot.reply_to(message, f"❌ Telegram API Error: File too large to download (~20MB limit).")
        else:
            bot.reply_to(message, f"❌ Telegram API Error: {str(e)}. Try later.")
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Unexpected error: {str(e)}")

# Zip file handling logic (replicated from old code)
def handle_zip_file(downloaded_file_content, file_name, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    temp_dir = None
    
    # Stop any currently running scripts for this user before uploading a ZIP
    scripts_to_stop = [k for k, v in bot_scripts.items() if v['script_owner_id'] == user_id]
    for script_key in scripts_to_stop:
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            del bot_scripts[script_key]
    
    # Delete old user files and folders (if any)
    try:
        if os.path.exists(user_folder):
             shutil.rmtree(user_folder)
             os.makedirs(user_folder)
             logger.info(f"Cleaned up user folder {user_folder} for ZIP upload.")
             
        # Clear database records for user files
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('DELETE FROM user_files WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
        if user_id in user_files: del user_files[user_id]
        logger.info(f"Cleared DB file records for user {user_id} for ZIP upload.")
    except Exception as e:
        logger.error(f"Error cleaning user folder/DB for ZIP upload {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error during cleanup: {str(e)}")
        return

    try:
        temp_dir = tempfile.mkdtemp()
        temp_zip_path = os.path.join(temp_dir, file_name)
        
        with open(temp_zip_path, 'wb') as f:
            f.write(downloaded_file_content)

        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(user_folder)
            
        logger.info(f"Extracted ZIP to {user_folder}")

        # Check for requirements.txt and package.json
        req_path = os.path.join(user_folder, 'requirements.txt')
        pkg_json = os.path.join(user_folder, 'package.json')
        
        main_script_name = None
        main_script_type = None
        
        # Determine main script
        extracted_files = os.listdir(user_folder)
        for f in extracted_files:
            f_lower = f.lower()
            if f_lower.endswith('.py') and f_lower not in ['add.py', 'main.py']:
                main_script_name = f
                main_script_type = 'py'
                break
            elif f_lower.endswith('.js') and f_lower not in ['add.js', 'main.js']:
                main_script_name = f
                main_script_type = 'js'
                break
        
        if not main_script_name:
            bot.reply_to(message, "⚠️ ZIP file must contain a single main Python (`.py`) or Javascript (`.js`) script.")
            return

        # Dependency Installation (replicated logic)
        if os.path.exists(req_path):
            req_file = os.path.basename(req_path)
            bot.reply_to(message, f"🔄 Installing Python deps from `{req_file}`...", parse_mode='Markdown')
            try:
                command = [sys.executable, '-m', 'pip', 'install', '-r', req_path]
                result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
                logger.info(f"pip install from requirements.txt OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Python deps from `{req_file}` installed.", parse_mode='Markdown')
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Python deps from `{req_file}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                error_msg = f"❌ Unexpected error installing Python deps: {e}"
                logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        if os.path.exists(pkg_json):
            pkg_file = os.path.basename(pkg_json)
            logger.info(f"package.json found, npm install in: {user_folder}")
            bot.reply_to(message, f"🔄 Installing Node deps from `{pkg_file}`...", parse_mode='Markdown')
            try:
                command = ['npm', 'install']
                result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=user_folder, encoding='utf-8', errors='ignore')
                logger.info(f"npm install OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Node deps from `{pkg_file}` installed.", parse_mode='Markdown')
            except FileNotFoundError:
                bot.reply_to(message, "❌ 'npm' not found. Cannot install Node deps. (Ensure Node.js is installed)"); 
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Node deps from `{pkg_file}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                error_msg = f"❌ Unexpected error installing Node deps: {e}"
                logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return
        
        # Save main script info and run it
        main_script_path = os.path.join(user_folder, main_script_name)
        save_user_file(user_id, main_script_name, main_script_type)
        
        if main_script_type == 'py':
            threading.Thread(target=run_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()
        elif main_script_type == 'js':
            threading.Thread(target=run_js_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()

    except zipfile.BadZipFile as e:
        logger.error(f"❌ Bad ZIP file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error: Invalid/corrupted ZIP. {e}")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing zip: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir); logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e: logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing JS file: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing Python file: {str(e)}")
# --- End Document Handler Assistants ---

# --- Callback Query Handlers (for Inline Buttons) ---
# ... (आपके सभी callback_query_handler functions जैसे handle_callbacks, start_bot_callback, stop_bot_callback, restart_bot_callback, आदि यहाँ रहेंगे)

def create_main_menu_inline(user_id):
    # ... (आपका create_main_menu_inline function यहाँ रहेगा)
    buttons = [
        types.InlineKeyboardButton("📢 Updates Channel", url=UPDATE_CHANNEL), #0
        types.InlineKeyboardButton("📤 Upload File", callback_data='upload'), #1
        types.InlineKeyboardButton("📂 Check Files", callback_data='check_files'), #2
        types.InlineKeyboardButton("⚡ Bot Speed", callback_data='speed'), #3
        types.InlineKeyboardButton("📞 Contact Owner", callback_data='contact_owner') #4
    ]
    markup = types.InlineKeyboardMarkup(row_width=2)
    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton("💳 Subscriptions", callback_data='subscriptions_panel'), #0
            types.InlineKeyboardButton("📊 Statistics", callback_data='stats'), #1
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot', callback_data='lock_bot' if not bot_locked else 'unlock_bot'), #2
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'), #3
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'), #4
            types.InlineKeyboardButton('🟢 Run All User Scripts', callback_data='run_all_scripts') #5
        ]
        markup.add(buttons[0]) 
        markup.add(buttons[1], buttons[2]) 
        markup.add(buttons[3], admin_buttons[0]) 
        markup.add(admin_buttons[1], admin_buttons[3]) 
        markup.add(admin_buttons[2], admin_buttons[5]) 
        markup.add(admin_buttons[4]) 
        markup.add(buttons[4]) 
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats'))
        markup.add(buttons[4])
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup
    
def create_control_buttons(script_owner_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
             types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}'),
             types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
    markup.row(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

# --- Callback Query Handlers ---

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    func_to_run(call)

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def check_files_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    user_files_list = user_files.get(user_id, [])
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        try:
            bot.edit_message_text("📂 Your files:\n\n(No files uploaded)", chat_id, call.message.message_id, reply_markup=markup)
        except Exception as e: logger.error(f"Error editing msg for empty file list: {e}")
        return
        
    bot.answer_callback_query(call.id)
    
    # Generate initial files list text
    file_status_messages = []
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) 
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        file_status_messages.append(f"• `{file_name}` ({file_type}) - **{status_icon}**")
        
    response_text = "📂 Your files:\n\n" + "\n".join(file_status_messages)
    
    # Create file control buttons
    markup_files = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) 
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup_files.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    
    markup_files.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    try:
        bot.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=markup_files, parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e): 
            logger.warning("Msg not modified (check_files).")
        else:
            logger.error(f"API error on check_files: {e}")
            
def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to control this script.", show_alert=True); return

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); return
        file_type = file_info[1]
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'

        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
            chat_id_for_reply, call.message.message_id, 
            reply_markup=create_control_buttons(script_owner_id, file_name, is_running), 
            parse_mode='Markdown'
        )
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e):
            logger.warning(f"Msg not modified (controls for {file_name})")
        else:
            raise
    except (ValueError, IndexError) as ve:
        logger.error(f"Error parsing file control callback: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "Error: Invalid action data.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in file_control_callback for data '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id
        logger.info(f"Start request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to start this script.", show_alert=True); return 

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return 
        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return 

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already running.", show_alert=True); return 

        bot.answer_callback_query(call.id, f"⏳ Starting {file_name} for user {script_owner_id}...")
        
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
            bot.send_message(chat_id_for_reply, f"❌ Unknown type '{file_type}' for '{file_name}'."); return

        time.sleep(1.5) 
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed)'
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
            if "message is not modified" in str(e): logger.warning(f"Msg not modified (start {file_name})")
            else: raise
    except Exception as e:
        logger.error(f"Error in start_bot_callback for data '{call.data}': {e}", exc_info=True)
        bot.send_message(chat_id_for_reply, f"❌ Error starting script: {e}")
        try: 
            _, script_owner_id_err, file_name_err = call.data.split('_', 2)
            bot.edit_message_text(f"⚙️ Controls for: `{file_name_err}` ({file_type}) of User `{script_owner_id_err}`\nStatus: 🔴 Stopped (Error)", chat_id_for_reply, call.message.message_id, reply_markup=create_control_buttons(int(script_owner_id_err), file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after start error: {e_btn}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id
        logger.info(f"Stop request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return 

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return 
        file_type = file_info[1]
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already stopped.", show_alert=True)
            try:
                bot.edit_message_text(
                    f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                    chat_id_for_reply, call.message.message_id, 
                    reply_markup=create_control_buttons(script_owner_id, file_name, False), 
                    parse_mode='Markdown')
            except Exception as e: logger.error(f"Error updating buttons (already stopped): {e}")
            return 

        bot.answer_callback_query(call.id, f"⏳ Stopping {file_name} for user {script_owner_id}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts: 
                del bot_scripts[script_key]; logger.info(f"Removed {script_key} from running after stop.")
            else: 
                logger.warning(f"Script {script_key} running by psutil but not in bot_scripts dict.")
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, False),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
            if "message is not modified" in str(e): logger.warning(f"Msg not modified (stop {file_name})")
            else: raise
    except Exception as e:
        logger.error(f"Error in stop_bot_callback for data '{call.data}': {e}", exc_info=True)
        bot.send_message(chat_id_for_reply, f"❌ Error stopping script: {e}")

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return 

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return 
        file_type = file_info[1]
        script_key = f"{script_owner_id}_{file_name}"
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return 

        # 1. Stop the current process (if running)
        if is_bot_running(script_owner_id, file_name):
            process_info = bot_scripts.get(script_key)
            if process_info:
                kill_process_tree(process_info)
                if script_key in bot_scripts: del bot_scripts[script_key]
        
        bot.answer_callback_query(call.id, f"⏳ Restarting script {script_key}...")
        
        # 2. Start the process
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
            bot.send_message(chat_id_for_reply, f"❌ Unknown type '{file_type}' for '{file_name}'."); return
            
        time.sleep(1.5)
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed)'
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
            if "message is not modified" in str(e): logger.warning(f"Msg not modified (restart {file_name})")
            else: raise
    except Exception as e:
        logger.error(f"Error in restart_bot_callback for data '{call.data}': {e}", exc_info=True)
        bot.send_message(chat_id_for_reply, f"❌ Error restarting script: {e}")

def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return 

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return 
        file_type = file_info[1]
        script_key = f"{script_owner_id}_{file_name}"
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        # 1. Stop the process (if running)
        if is_bot_running(script_owner_id, file_name):
            process_info = bot_scripts.get(script_key)
            if process_info:
                kill_process_tree(process_info)
                if script_key in bot_scripts: del bot_scripts[script_key]
        
        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name} for user {script_owner_id}...")

        # 2. Delete file from file system
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted file {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")
                bot.send_message(chat_id_for_reply, f"❌ Error deleting file from system: {e}")
                return

        # 3. Delete file from database
        remove_user_file_db(script_owner_id, file_name)

        # 4. Check if user folder is empty and delete it if so
        if not os.listdir(user_folder):
            try:
                shutil.rmtree(user_folder)
                logger.info(f"Removed empty user folder {user_folder}")
            except Exception as e:
                logger.error(f"Error removing empty user folder {user_folder}: {e}")
                
        bot.send_message(chat_id_for_reply, f"✅ Script `{file_name}` deleted successfully.", parse_mode='Markdown')
        # Redirect to updated file list
        check_files_callback(call)
    except Exception as e:
        logger.error(f"Error in delete_bot_callback for data '{call.data}': {e}", exc_info=True)
        bot.send_message(chat_id_for_reply, f"❌ Error deleting script: {e}")

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return 

        file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); return 

        bot.answer_callback_query(call.id, f"📜 Fetching logs for {file_name}...")
        
        user_folder = get_user_folder(script_owner_id)
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if not os.path.exists(log_file_path):
            bot.send_message(chat_id_for_reply, f"📜 Log file for `{file_name}` not found. Has the script run recently?", parse_mode='Markdown')
            return

        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            logs = f.read()

        if not logs.strip():
            bot.send_message(chat_id_for_reply, f"📜 Log file for `{file_name}` is empty.", parse_mode='Markdown')
            return

        # Send logs as a file if too long
        if len(logs) > 4000:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8', errors='ignore', suffix='.log') as tmp_file:
                tmp_file.write(logs)
                tmp_path = tmp_file.name
            
            with open(tmp_path, 'rb') as doc_file:
                bot.send_document(chat_id_for_reply, doc_file, caption=f"📜 Logs for `{file_name}` (Too long for message).", parse_mode='Markdown')
            os.remove(tmp_path)
            
        else:
            bot.send_message(chat_id_for_reply, f"📜 **Logs for `{file_name}`:**\n\n```\n{logs.strip()}\n```", parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error in logs_bot_callback for data '{call.data}': {e}", exc_info=True)
        bot.send_message(chat_id_for_reply, f"❌ Error fetching logs: {e}")

def stats_callback(call):
    bot.answer_callback_query(call.id)
    _logic_statistics(call)
    
def speed_callback(call):
    bot.answer_callback_query(call.id)
    _logic_bot_speed(call)

def contact_owner_callback(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, f"📞 Owner Contact:\n\nTelegram Username: {YOUR_USERNAME}")

def toggle_lock_callback(call):
    global bot_locked
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True); return
    
    bot_locked = not bot_locked
    status = "🔒 LOCKED" if bot_locked else "🔓 UNLOCKED"
    bot.answer_callback_query(call.id, f"Bot is now {status}!", show_alert=True)
    
    # Update button text in the main menu
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: logger.error(f"Error updating main menu button after lock toggle: {e}")

def run_all_scripts_callback(call):
    admin_required_callback(call, _logic_run_all_scripts)

def subscription_management_callback(call):
    admin_required_callback(call, lambda c: _logic_subscriptions_panel(c.message))

def admin_panel_callback(call):
    owner_required_callback(call, lambda c: _logic_admin_panel(c.message))

def back_to_main_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    
    expiry_info = ""
    user_status = "🆓 Free User"
    if user_id == OWNER_ID:
        user_status = "👑 Owner"
    elif user_id in admin_ids:
        user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            days_left = (expiry_date - datetime.now()).days 
            user_status = "⭐ Premium"
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else:
            user_status = "🆓 Free User (Expired Sub)" 

    main_menu_text = (f"〽️ Welcome back, {call.from_user.first_name}!\n\n🆔 ID: `{user_id}`\n"
                      f"🔰 Status: {user_status}{expiry_info}\n📁 Files: {current_files} / {limit_str}\n\n"
                      f"👇 Use buttons or type commands.")
    
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(main_menu_text, chat_id, call.message.message_id, 
                              reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e): logger.warning("Msg not modified (back_to_main).")
        else: logger.error(f"API error on back_to_main: {e}")
    except Exception as e:
        logger.error(f"Error handling back_to_main: {e}", exc_info=True)

# ... (बाकी सभी Next Step Handlers - process_add_sub_id, process_broadcast_message, आदि यहाँ रहेंगे)
# --- Next Step Handlers ---
def process_broadcast_message(message):
    if message.from_user.id not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return 
    if message.text and message.text.lower() == '/cancel': bot.reply_to(message, "Broadcast cancelled."); return 

    broadcast_content = message.text 
    if not broadcast_content and not (message.photo or message.video or message.document or message.sticker or message.voice or message.audio): 
        bot.reply_to(message, "⚠️ Cannot broadcast empty message. Send text or media, or /cancel.")
        msg = bot.send_message(message.chat.id, "📢 Send broadcast message or /cancel.")
        bot.register_next_step_handler(msg, process_broadcast_message)
        return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(types.InlineKeyboardButton("✅ Confirm & Send", callback_data=f"confirm_broadcast_{message.message_id}"),
               types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast"))
               
    preview_text = broadcast_content[:1000].strip() if broadcast_content else "(Media message)"
    bot.reply_to(message, f"⚠️ Confirm Broadcast:\n\n```\n{preview_text}\n```\n"
                          f"To **{target_count}** users. Sure?", reply_markup=markup, parse_mode='Markdown')
                          
def process_add_sub_id(message):
    # ... (process_add_sub_id logic)
    if message.from_user.id not in admin_ids: bot.reply_to(message, "⚠️ Admin only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Subscription cancelled."); return
    
    try:
        target_user_id = int(message.text)
        if target_user_id <= 0: raise ValueError
        
        if target_user_id in user_subscriptions:
            current_expiry = user_subscriptions[target_user_id]['expiry']
            bot.reply_to(message, f"⚠️ User `{target_user_id}` already has a subscription expiring on `{current_expiry.isoformat()}`. Extending it or removing it first is recommended.")
            msg = bot.send_message(message.chat.id, "Enter number of days to extend/set subscription (e.g., 30, 90, 365) or /cancel.")
            bot.register_next_step_handler(msg, lambda m: process_sub_days(m, target_user_id, extend_mode=True))
        else:
            bot.reply_to(message, f"✅ Target User ID set: `{target_user_id}`. Now enter the subscription duration in **days** (e.g., 30, 90, 365) or /cancel.")
            msg = bot.send_message(message.chat.id, "Enter number of days to set subscription.")
            bot.register_next_step_handler(msg, lambda m: process_sub_days(m, target_user_id, extend_mode=False))

    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to add subscription or /cancel.")
        bot.register_next_step_handler(msg, process_add_sub_id)

def process_sub_days(message, target_user_id, extend_mode=False):
    # ... (process_sub_days logic)
    if message.from_user.id not in admin_ids: bot.reply_to(message, "⚠️ Admin only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Subscription cancelled."); return
    
    try:
        days = int(message.text)
        if days <= 0: raise ValueError
        
        if extend_mode and target_user_id in user_subscriptions:
            # Extend from current expiry, or from now if already expired
            base_date = user_subscriptions[target_user_id]['expiry']
            if base_date < datetime.now():
                base_date = datetime.now()
            new_expiry = base_date + timedelta(days=days)
            action_text = "Extended"
        else:
            # Set from now
            new_expiry = datetime.now() + timedelta(days=days)
            action_text = "Set"

        save_subscription(target_user_id, new_expiry)
        
        bot.reply_to(message, f"✅ Subscription **{action_text}** for User `{target_user_id}`. New Expiry: `{new_expiry.isoformat().split('.')[0]}` ({days} days).", parse_mode='Markdown')
        try:
             bot.send_message(target_user_id, f"⭐ Your Premium subscription has been **{action_text.lower()}**! New Expiry: `{new_expiry.strftime('%Y-%m-%d %H:%M:%S')}`. Enjoy!", parse_mode='Markdown')
        except telebot.apihelper.ApiTelegramException as e:
            logger.warning(f"Failed to notify user {target_user_id} of sub change: {e}")
            
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid number of days. Send a positive integer or /cancel.")
        msg = bot.send_message(message.chat.id, "Enter number of days to set subscription.")
        bot.register_next_step_handler(msg, lambda m: process_sub_days(m, target_user_id, extend_mode))

def process_remove_sub_id(message):
    # ... (process_remove_sub_id logic)
    if message.from_user.id not in admin_ids: bot.reply_to(message, "⚠️ Admin only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Subscription removal cancelled."); return
    
    try:
        target_user_id = int(message.text)
        if target_user_id <= 0: raise ValueError
        
        if target_user_id not in user_subscriptions:
            bot.reply_to(message, f"⚠️ User `{target_user_id}` does not have an active subscription.", parse_mode='Markdown')
            return
            
        remove_subscription_db(target_user_id)
        
        bot.reply_to(message, f"✅ Subscription removed for User `{target_user_id}`.", parse_mode='Markdown')
        try:
            bot.send_message(target_user_id, "😔 Your Premium subscription has been removed.", parse_mode='Markdown')
        except telebot.apihelper.ApiTelegramException as e:
            logger.warning(f"Failed to notify user {target_user_id} of sub removal: {e}")
            
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to remove subscription or /cancel.")
        bot.register_next_step_handler(msg, process_remove_sub_id)

def process_add_admin_id(message):
    # ... (process_add_admin_id logic)
    if message.from_user.id != OWNER_ID: bot.reply_to(message, "⚠️ Owner only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Admin addition cancelled."); return
    
    try:
        new_admin_id = int(message.text)
        if new_admin_id <= 0: raise ValueError
        
        if new_admin_id in admin_ids:
            bot.reply_to(message, f"⚠️ User `{new_admin_id}` is already an admin.", parse_mode='Markdown'); return
            
        add_admin_db(new_admin_id)
        bot.reply_to(message, f"✅ User `{new_admin_id}` promoted to Admin.", parse_mode='Markdown')
        try:
            bot.send_message(new_admin_id, "🛡️ You have been promoted to **Admin** status!", parse_mode='Markdown')
        except Exception as e: logger.warning(f"Failed to notify new admin {new_admin_id}: {e}")
            
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter User ID to promote or /cancel.")
        bot.register_next_step_handler(msg, process_add_admin_id)
    except Exception as e:
        logger.error(f"Error processing add admin: {e}", exc_info=True); bot.reply_to(message, "Error.")

def process_remove_admin_id(message):
    # ... (process_remove_admin_id logic)
    if message.from_user.id != OWNER_ID: bot.reply_to(message, "⚠️ Owner only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Admin removal cancelled."); return
    
    try:
        admin_id_remove = int(message.text)
        if admin_id_remove <= 0: raise ValueError
        
        if admin_id_remove == OWNER_ID:
            bot.reply_to(message, "⚠️ Cannot remove the bot **Owner**.", parse_mode='Markdown'); return
            
        if admin_id_remove not in admin_ids:
            bot.reply_to(message, f"⚠️ User `{admin_id_remove}` is not currently an admin.", parse_mode='Markdown'); return
            
        remove_admin_db(admin_id_remove)
        bot.reply_to(message, f"✅ User `{admin_id_remove}` removed from Admin status.", parse_mode='Markdown')
        try:
            bot.send_message(admin_id_remove, "💀 You have been removed from **Admin** status.", parse_mode='Markdown')
        except Exception as e: logger.warning(f"Failed to notify removed admin {admin_id_remove}: {e}")
            
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter User ID of Admin to remove or /cancel.")
        bot.register_next_step_handler(msg, process_remove_admin_id)
    except Exception as e:
        logger.error(f"Error processing remove admin: {e}", exc_info=True); bot.reply_to(message, "Error.")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")
    
    # Global lock check
    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats']: 
        bot.answer_callback_query(call.id, "⚠️ Bot locked by admin.", show_alert=True)
        return
        
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        
        # Admin/Owner callbacks
        elif data == 'stats': stats_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'contact_owner': contact_owner_callback(call)
        elif data == 'lock_bot' or data == 'unlock_bot': toggle_lock_callback(call)
        elif data == 'run_all_scripts': run_all_scripts_callback(call)
        elif data == 'subscriptions_panel': subscription_management_callback(call)
        elif data == 'admin_panel': admin_panel_callback(call)
        
        # Subscriptions management handlers (next step initiators)
        elif data == 'add_sub_init': 
            admin_required_callback(call, lambda c: bot.register_next_step_handler(bot.send_message(c.message.chat.id, "💳 Enter User ID to add subscription or /cancel."), process_add_sub_id))
        elif data == 'remove_sub_init':
            admin_required_callback(call, lambda c: bot.register_next_step_handler(bot.send_message(c.message.chat.id, "💳 Enter User ID to remove subscription or /cancel."), process_remove_sub_id))
        elif data == 'list_subs':
            admin_required_callback(call, list_subscriptions_callback)
            
        # Broadcast callbacks
        elif data == 'broadcast': 
            admin_required_callback(call, lambda c: _logic_broadcast_init(c.message))
        elif data.startswith('confirm_broadcast_'): 
            admin_required_callback(call, handle_confirm_broadcast)
        elif data == 'cancel_broadcast':
             admin_required_callback(call, lambda c: bot.answer_callback_query(c.id, "Broadcast cancelled.", show_alert=True))

        # Owner Admin Panel callbacks
        elif data == 'add_admin_init':
            owner_required_callback(call, lambda c: bot.register_next_step_handler(bot.send_message(c.message.chat.id, "👑 Enter User ID to promote or /cancel."), process_add_admin_id))
        elif data == 'remove_admin_init':
            owner_required_callback(call, lambda c: bot.register_next_step_handler(bot.send_message(c.message.chat.id, "👑 Enter User ID of Admin to remove or /cancel."), process_remove_admin_id))
        elif data == 'list_admins':
             owner_required_callback(call, list_admins_callback)
        elif data == 'stop_all_scripts':
             owner_required_callback(call, handle_stop_all_scripts)
        
        # Navigation
        elif data == 'back_to_main': back_to_main_callback(call)
        
        else: bot.answer_callback_query(call.id, "⚠️ Invalid option.", show_alert=True)
        
    except Exception as e:
        logger.error(f"❌ Error handling callback '{data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, f"❌ An internal error occurred: {str(e)[:50]}", show_alert=True)
        
# --- Additional Admin/Owner Handlers (Replicated for completeness) ---

def list_subscriptions_callback(call):
    # Function to list active subscriptions
    bot.answer_callback_query(call.id)
    active_subs = [f"• User ID: `{uid}` - Expires: `{data['expiry'].strftime('%Y-%m-%d')}`" 
                   for uid, data in user_subscriptions.items() if data['expiry'] > datetime.now()]
                   
    if not active_subs:
        msg = "📜 No active subscriptions found."
    else:
        msg = "📜 **Active Subscriptions:**\n\n" + "\n".join(active_subs)
        
    bot.send_message(call.message.chat.id, msg, parse_mode='Markdown')

def list_admins_callback(call):
    # Function to list current admins
    bot.answer_callback_query(call.id)
    admin_list = [f"• User ID: `{uid}` {'(OWNER)' if uid == OWNER_ID else ''}" 
                  for uid in admin_ids]
                  
    if not admin_list:
        msg = "📜 No admins found (This shouldn't happen)."
    else:
        msg = "📜 **Current Admins:**\n\n" + "\n".join(admin_list)
        
    bot.send_message(call.message.chat.id, msg, parse_mode='Markdown')

def handle_confirm_broadcast(call):
    # Function to execute broadcast
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    try:
        original_message = call.message.reply_to_message
        if not original_message: raise ValueError("Could not retrieve original message.") 

        broadcast_text = original_message.text 
        broadcast_photo_id = original_message.photo[-1].file_id if original_message.photo else None
        broadcast_video_id = original_message.video.file_id if original_message.video else None
        caption = original_message.caption 
        
        total_users = len(active_users)
        bot.answer_callback_query(call.id, f"📢 Broadcasting to {total_users} users...")
        bot.edit_message_text(f"📢 Broadcast initiated! Sending to {total_users} users...", chat_id, call.message.message_id)

        sent_count = 0
        blocked_count = 0
        failed_count = 0
        batch_size = 10 
        delay_batches = 3 
        
        user_list = list(active_users)
        
        for i, user_id_bc in enumerate(user_list):
            try:
                if broadcast_text:
                    bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
                elif broadcast_photo_id:
                    bot.send_photo(user_id_bc, broadcast_photo_id, caption=caption, parse_mode='Markdown' if caption else None)
                elif broadcast_video_id:
                    bot.send_video(user_id_bc, broadcast_video_id, caption=caption, parse_mode='Markdown' if caption else None)
                sent_count += 1
            except telebot.apihelper.ApiTelegramException as e:
                err_desc = str(e).lower()
                if any(s in err_desc for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from", "restricted"]):
                    logger.warning(f"Broadcast failed to {user_id_bc}: User blocked/inactive.")
                    blocked_count += 1
                elif "flood control" in err_desc or "too many requests" in err_desc:
                    retry_after = 5
                    match = re.search(r"retry after (\d+)", err_desc)
                    if match: retry_after = int(match.group(1)) + 1
                    logger.warning(f"Flood control. Sleeping {retry_after}s...")
                    time.sleep(retry_after)
                    # Retry once
                    try:
                         if broadcast_text: bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
                         elif broadcast_photo_id: bot.send_photo(user_id_bc, broadcast_photo_id, caption=caption, parse_mode='Markdown' if caption else None)
                         elif broadcast_video_id: bot.send_video(user_id_bc, broadcast_video_id, caption=caption, parse_mode='Markdown' if caption else None)
                         sent_count += 1
                    except Exception as e_retry: logger.error(f"Broadcast retry failed to {user_id_bc}: {e_retry}"); failed_count +=1
                else:
                    logger.error(f"Broadcast failed to {user_id_bc}: {e}"); failed_count += 1
            except Exception as e:
                logger.error(f"Unexpected error broadcasting to {user_id_bc}: {e}"); failed_count += 1

            if (i + 1) % batch_size == 0 and i < total_users - 1:
                 logger.info(f"Broadcast batch {i//batch_size + 1} sent. Sleeping {delay_batches}s...")
                 time.sleep(delay_batches)
            elif i % 5 == 0: time.sleep(0.1) # Small delay to avoid basic flood limit

        final_msg = (f"✅ Broadcast Complete!\n\n"
                     f"📢 Total Users: `{total_users}`\n"
                     f"🟢 Sent Successfully: `{sent_count}`\n"
                     f"🚫 Blocked/Inactive: `{blocked_count}`\n"
                     f"❌ Failed (Other Errors): `{failed_count}`")

        bot.edit_message_text(final_msg, chat_id, call.message.message_id, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"❌ Error during broadcast execution: {e}", exc_info=True)
        bot.send_message(chat_id, f"❌ Error during broadcast execution: {e}")

def handle_stop_all_scripts(call):
    # Function to stop all running scripts
    bot.answer_callback_query(call.id, "⏳ Stopping all user scripts...")
    chat_id = call.message.chat.id
    
    scripts_to_stop = list(bot_scripts.keys())
    stopped_count = 0
    
    if not scripts_to_stop:
        bot.edit_message_text("❌ No scripts were running to stop.", chat_id, call.message.message_id)
        return
        
    bot.edit_message_text(f"⏳ Stopping {len(scripts_to_stop)} scripts. Please wait...", chat_id, call.message.message_id)

    for key in scripts_to_stop:
        if key in bot_scripts:
            try:
                kill_process_tree(bot_scripts[key])
                del bot_scripts[key]
                stopped_count += 1
            except Exception as e:
                logger.error(f"Error stopping script {key} during mass stop: {e}")

    bot.edit_message_text(f"✅ Successfully stopped **{stopped_count}** scripts.", chat_id, call.message.message_id, parse_mode='Markdown')


# --- CLEANUP FUNCTION (Called on graceful exit) ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys())
    if not script_keys_to_stop: 
        logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts:
            logger.info(f"Stopping: {key}"); 
            kill_process_tree(bot_scripts[key])
            # Do NOT delete from bot_scripts here, as kill_process_tree might already do it
            
atexit.register(cleanup)


# --- FLASK WEBHOOK SETUP (Polling को Webhook से बदलें) ---
app = Flask(__name__)

# Webhook Path: सुरक्षा के लिए, हम Bot Token को ही URL का गुप्त हिस्सा बनाते हैं।
# Render पर आपका Webhook URL ऐसा होगा: https://your-service-name.onrender.com/<TOKEN>
WEBHOOK_PATH = f"/{TOKEN}" 

@app.route('/')
def home():
    # यह Render के Health Check के लिए है।
    return "I'm FKS File Host Bot running via Webhook!"

@app.route(WEBHOOK_PATH, methods=['POST'])
def webhook():
    # Telegram से आए POST request को handle करें
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        # bot.process_new_updates को एक अलग Thread में चलाएं ताकि Flask request time out न हो
        threading.Thread(target=bot.process_new_updates, args=([update],)).start()
        return 'ok', 200
    return 'not json', 403

# --- MAIN EXECUTION (Gunicorn के लिए) ---
# Gunicorn इस फाइल को चलाएगा (ADD.py) और 'app' वेरिएबल को Web Server के रूप में उपयोग करेगा।
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot configured for Render Webhook.\n" + 
                f"🔧 Base Dir: {BASE_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + 
                "⚡ Use 'gunicorn ADD:app' as the Start Command on Render.\n" + "="*40)
    # Production पर Gunicorn इस block को run नहीं करेगा, बल्कि सीधे 'app' variable को लोड करेगा।
    # Local Testing के लिए आप app.run() का उपयोग कर सकते हैं।
    # app.run(host='0.0.0.0', port=os.environ.get("PORT", 8080))
