import requests
import json
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from logger import Logger

load_dotenv()
logger = Logger()

class BFFCuenta:
    """
    Singleton class to manage API calls to the BFF Cuenta endpoint.
    Ensures only one instance of the client is created and manages the API URL.
    """
    _instance = None
    API_URL = os.getenv("URL_BFF_CUENTA")
    auth_token = os.getenv("TOKEN_BFF_CUENTA")

    def __new__(cls, *args, **kwargs):
        """Implement the Singleton pattern to ensure only one instance exists."""
        if cls._instance is None:
            # Create the instance only if it doesn't exist
            cls._instance = super(BFFCuenta, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initialization logic runs here. 
        Note: For a simple Singleton, this method runs every time, 
        but we don't need complex logic that must run only once.
        """
        pass

    def fetch_account_data(self) -> Optional[Dict[str, Any]]:
        """
        Makes an authenticated GET request to the specified API endpoint.

        Args:
            auth_token: The JWT or Bearer token required for authorization.

        Returns:
            A dictionary containing the JSON response data, or None if the request failed.
        """
        
        # Construct the Authorization header using the Bearer scheme
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Attempting to fetch data from: {self.API_URL}")

        try:
            # Make the GET request
            response = requests.get(self.API_URL, headers=headers, timeout=10)

            # Raise an HTTPError for bad responses (4xx or 5xx status codes)
            response.raise_for_status()

            # If the request was successful (status code 200-299), parse the JSON
            data = response.json()
            logger.info("✅ Successfully fetched and parsed data.")
            return data

        except requests.exceptions.HTTPError as e:
            # Handle HTTP-specific errors (401 Unauthorized, 404 Not Found, 500 Server Error)
            logger.error(f" HTTP Error encountered: {e}")
            # Access response text via the exception object (safer practice)
            logger.error(f"Response Body (if available): {e.response.text}")
            return None
            
        except requests.exceptions.ConnectionError:
            logger.error("Error connecting to the API service. Check network/URL.")
            return None
            
        except requests.exceptions.Timeout:
            logger.error("Request timed out after 10 seconds.")
            return None
            
        except requests.exceptions.RequestException as e:
            # Catch any other generic requests error (e.g., DNS errors)
            logger.error(f"An unexpected request error occurred: {e}")
            return None
        
def get_bff_cuenta_instance() -> BFFCuenta:
    """
    Returns the Singleton instance of the BFFCuenta client.
    
    This function implements the requested syntax 'bff_instance = get_instance()' 
    and leverages the BFFCuenta's __new__ method to ensure Singleton behavior.
    """
    return BFFCuenta()