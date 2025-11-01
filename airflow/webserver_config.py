"""
Custom Airflow webserver configuration for no-authentication mode
This allows direct access to Airflow UI without login
"""
import os
from flask_appbuilder.security.manager import AUTH_DB

# Use database authentication
AUTH_TYPE = AUTH_DB

# Public role gets Admin access - everyone can access without login
AUTH_ROLE_PUBLIC = 'Admin'

APP_THEME = "simplex.css"  

# Disable CSRF for easier API access
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = None

ENABLE_PROXY_FIX = True

PERMANENT_SESSION_LIFETIME = 604800  

AUTH_ROLE_ADMIN = 'Admin'

# Allow user registration and auto-assign Admin role
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Admin"

EXPOSE_CONFIG = True

AUTH_RATE_LIMITED = False
AUTH_RATE_LIMIT = "10000 per hour"

