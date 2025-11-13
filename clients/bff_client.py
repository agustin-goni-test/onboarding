import requests
import json
import os
from thefuzz import fuzz
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from logger import Logger

load_dotenv()
logger = Logger()


class ReferenceItem(BaseModel):
    '''
    Models a name and code pair for the information obtained in the service.
    '''
    code: int
    name: str

class AccountReference(BaseModel):
    '''
    This class contains the response of the entry BFF method.
    Holds banks and account types. Disregards bank account.
    '''
    banks: List[ReferenceItem] = Field(description="List of banks with their codes.")
    accountTypes: List[ReferenceItem] = Field(description="List of possible account types with their codes.")

    @classmethod
    def from_api_response(cls, api_response: dict) -> 'AccountReference':
        '''
        Create the object that holds the information from the endpoint response.
        '''
        return cls(**api_response)

class EconomicActivity(BaseModel):
    """Models a single activity object from the 'data' list."""
    id: int
    code: int
    name: str
    enabled: int # Use int to match the JSON, though it acts like a boolean

class EconomicActivitiesResponse(BaseModel):
    """Models the full response structure for the activity endpoint."""
    date: str
    message: str
    data: List[EconomicActivity]

    @classmethod
    def from_api_response(cls, api_response: dict) -> 'EconomicActivitiesResponse':
        '''
        Create the object that holds the information from the endpoint response.
        '''
        return cls(**api_response)

class BFFCuenta:
    """
    Singleton class to manage API calls to the BFF Cuenta endpoint.
    Ensures only one instance of the client is created and manages the API URL.
    """
    _instance = None
    API_URL = os.getenv("URL_BFF_CUENTA")
    auth_token = os.getenv("TOKEN_BFF_CUENTA")
    data: AccountReference = None

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
            logger.info("Successfully fetched and parsed data.")
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
        
    def populate_account_data(self):
        '''Populates the account types and bank list in data.
        Used for obtaining the codes.'''
        json_response = self.fetch_account_data()
        self.data = AccountReference.from_api_response(json_response)
        logger.info("Información de cuentas obtenida con éxito...")

    def obtain_account_type_code(self, account_type: str) -> int:
        # First, determine if data is present. If not, return None
        if not self.data:
            return None    
        
        # Iterate through the account types looking for a match
        for item in self.data.accountTypes:
            if item.name.lower() == account_type.lower():
                return item.code
        
        # If nothing is found, return None
        return None
    
    def obtain_bank_code(self, bank: str) -> int:
        # First, determine if data is present. If not, return None
        if not self.data:
            return None
        
        # Find bank where name matches the one found
        for item in self.data.banks:
            if item.name.lower() == bank.lower():
                return item.code
            
        # If we can't get a perfect match, try a partial match
        for item in self.data.banks:
            if bank.lower() in item.name.lower():
                return item.code
            
        # If nothing is found, return None
        return None

    
        
def get_bff_cuenta_instance() -> BFFCuenta:
    """
    Returns the Singleton instance of the BFFCuenta client.
    
    This function implements the requested syntax 'bff_instance = get_instance()' 
    and leverages the BFFCuenta's __new__ method to ensure Singleton behavior.
    """
    return BFFCuenta()


class BFFComercio:
    """
    Singleton class to manage API calls to the BFF Comercio endpoint.
    Ensures only one instance of the client is created and manages the API URL.
    """
    _instance = None
    API_URL = os.getenv("BASE_URL_BFF_COMERCIO")
    auth_token = os.getenv("TOKEN_BFF_COMERCIO")
    EC_ACTIVITIES_URL = os.getenv("ALL_EC_ACTIVITIES")
    MCC_URL = os.getenv("MCC")
    activities: EconomicActivitiesResponse = None


    def __new__(cls, *args, **kwargs):
        """Implement the Singleton pattern to ensure only one instance exists."""
        if cls._instance is None:
            # Create the instance only if it doesn't exist
            cls._instance = super(BFFComercio, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """
        Initialization logic runs here. 
        Note: For a simple Singleton, this method runs every time, 
        but we don't need complex logic that must run only once.
        """
        pass

    def fetch_all_economic_activities(self) -> Optional[Dict[str, Any]]:
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
        
        endpoint_url = self.API_URL + self.EC_ACTIVITIES_URL
        logger.info(f"Attempting to fetch data from: {endpoint_url}")

        try:
            # Make the GET request
            response = requests.get(endpoint_url, headers=headers, timeout=10)

            # Raise an HTTPError for bad responses (4xx or 5xx status codes)
            response.raise_for_status()

            # If the request was successful (status code 200-299), parse the JSON
            data = response.json()
            logger.info("Successfully fetched and parsed data.")
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
        
    def fetch_mcc_info(self, code: int) -> Optional[Dict[str, Any]]:
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
        
        endpoint_url = self.API_URL + self.MCC_URL + str(code)
        logger.info(f"Attempting to fetch data from: {endpoint_url}")

        try:
            # Make the GET request
            response = requests.get(endpoint_url, headers=headers, timeout=10)

            # Raise an HTTPError for bad responses (4xx or 5xx status codes)
            response.raise_for_status()

            # If the request was successful (status code 200-299), parse the JSON
            data = response.json()
            logger.info("Successfully fetched and parsed data.")
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
        
    def get_giro_and_mcc(self, code: int) -> Tuple[int, int]:
        '''Obtain values from service'''
        json_response = self.fetch_mcc_info(code)
        mcc = json_response[0].get("mcc")
        giro = json_response[0].get("idGiro")
        return mcc, giro
        
    def populate_economic_activities(self):
        json_response = self.fetch_all_economic_activities()
        self.activities = EconomicActivitiesResponse.from_api_response(json_response)
        logger.info("Información de actividades económicas obtenida con éxito...")

    def obtain_activity_code(self, activity: str) -> int:
        # If there are no data, return None
        if not self.activities:
            return None
        
        # Standarize to eliminate variation
        standardized_input = self._standardize_name(activity)
        
        # Iterate through all activities to find a match
        # Try exact match
        for item in self.activities.data:
            standardized_activity_name = self._standardize_name(item.name)
            if standardized_activity_name == standardized_input:
                if item.enabled == 1:
                    return item.code
                
        # If there is no match, try a fuzzy approach
        logger.info("No encontramos acitividad económica idéntica. Usaremos lógica difusa...")
        best_score = 0
        best_match_code = None

        for item in self.activities.data:
            if item.enabled == 1:
                # Use fuzz.ratio for simple similarity, or fuzz.token_set_ratio
                # for more accuracy
                standardized_activity_name = self._standardize_name(item.name)
                score = fuzz.token_set_ratio(standardized_input, standardized_activity_name)

                if score > best_score:
                    best_score = score
                    best_match_code = item.code

                if best_score > 80:
                    logger.info(f"Usando lógica difusa obtuvimos el mejor score de {best_score} para {activity}...")
                    return best_match_code
                
        # If nothing found, return None
        return None
    
    def _standardize_name(self, name: str) -> str:
        # Simple method to remove common Spanish accents for basic normalization
        name = name.lower().strip()
        name = name.replace('á', 'a').replace('é', 'e').replace('í', 'i')
        name = name.replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')
        return name


        
def get_bff_comercio_instance() -> BFFComercio:
    """
    Returns the Singleton instance of the BFFCuenta client.
    
    This function implements the requested syntax 'bff_instance = get_instance()' 
    and leverages the BFFComercio's __new__ method to ensure Singleton behavior.
    """
    return BFFComercio()
    