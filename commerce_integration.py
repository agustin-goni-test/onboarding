from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from document_capture import InformationNode

# --- Sub-Component Classes ---

class IntegrationAddress(BaseModel):
    """Models the address structure used in Commerce and Branches."""
    region: int
    comune: int
    number: str
    fullAddress: List[str]
    addressWithoutNumber: str

class IntegrationTerminals(BaseModel):
    """Models the terminal configuration details."""
    commerceRut: str
    branchCode: int
    terminalId: Optional[str] = None # Assuming null means Optional[str]
    contractId: str
    technology: int
    ussdNumber: int
    user: str
    obs: str
    additionalInfo: str
    serviceId: int
    sellerRut: str
    terminalNumber: str
    configurationType: str

# --- Main Component Classes ---

class IntegrationCommerce(BaseModel):
    """Models the primary commerce integration details."""
    commerceRut: str
    businessName: str
    businessLine: int
    origin: str
    email: str
    emailPayment: str
    fantasyName: str
    name: str
    lastName: str
    mothersLastName: str
    mobilePhoneNumber: str
    sellerRut: str
    integrationAddress: IntegrationAddress # Nested object
    obs: str

class IntegrationBankAccount(BaseModel):
    """Models the bank account details."""
    commerceRut: str
    bankCode: int
    ownerFullName: str
    ownerRut: str
    ownerEmail: str
    user: str
    accountType: int
    ownerAccountNumber: str
    serviceId: int
    paymentType: str

class IntegrationContact(BaseModel):
    """Models the integration contact details."""
    commerceRut: str
    legalRepresentative: bool
    names: str
    lastName: str
    secondLastName: str
    rut: str
    email: str
    phone: str
    serialNumber: str
    sign: bool
    third: bool
    signAllowed: bool

class IntegrationBranches(BaseModel):
    """Models the branch details, which includes an address and terminals list."""
    branchId: Optional[str] = None
    mainBranch: bool
    branchVerticalId: int
    integrationAddress: IntegrationAddress  # Nested object
    businessName: str
    commerceRut: str
    email: str
    fantasyName: str
    description: str
    idMcc: int
    mobilePhoneNumber: str
    name: str
    webSite: str
    mantisaBill: str
    dvBill: str
    bankAccount: str
    mantisaHolder: str
    integrationType: str
    user: str
    emailContact: str
    merchantType: int
    commerceContactName: str
    commerceLegalRepresentativeName: str
    commerceLegalRepresentativeRut: str
    commerceLegalRepresentativePhone: str
    integrationTerminals: List[IntegrationTerminals] # Nested list of objects

# --- Top-Level Class ---

class EntidadesVolcado(BaseModel):
    """The top-level object containing the entire JSON structure."""
    integrationCommerce: IntegrationCommerce
    integrationBankAccount: IntegrationBankAccount
    integrationContact: IntegrationContact
    integrationBranches: List[IntegrationBranches]

    def to_json(self, **kwargs) -> str:
        """
        Custom method to fulfill the request for a 'to_json' method.
        It uses Pydantic's model_dump_json for complete, cascaded serialization.
        """
        # Pydantic's model_dump_json automatically cascades to all nested BaseModels
        # By default, it returns a JSON string.
        return self.model_dump_json(**kwargs)
    
class VolcadoManager:

    def __init__(self, inference_results: Dict[str, InformationNode]):
        '''
        Initializes the manager with the final consolidated extraction results.

        Args:
            document_results: The 'results' attribute from DocumentCaptureState,
                              which is a Dict[field_name, InformationNode]
        '''
        # If results come as string, convert to dict
        if isinstance(inference_results, str):
            self.results = self._convert_string_to_dict(inference_results)
        else:
            self.results = inference_results

        self.volcado_data: Dict[str, Any] = {}


    def _convert_string_to_dict(self, results_string: str) -> Dict[str, Any]:
        '''Convert the string representation of results back to dictionary'''
        try:
            # Simple approach: use eval (be careful with untrusted data)
            # Only use this if you control the data source
            return eval(results_string)
        except:
            # Fallback: manual parsing or return empty dict
            print("Warning: Could not parse results string")
            return {}


    def _get_value(self, field_key: str, default: Any = None) -> Any:
        '''
        Helper method to retrieve values from the results of the inference.
        Handles InformationNode structure.
        '''
        node = self.results.get(field_key)
        if not node:
            return default
        else:
            return node.get("value", default)
        
    def complete_results(self) -> Dict[str, Any]:
        missing_fields = []
        
        # Search for fields with no matches
        for field_name, node in self.results.items():
            if not node.get("value"):
                missing_fields.append(field_name)

        # If found, ask the user to provide them
        if missing_fields:
            print("Debemos completar la información obtenida desde la inferencia")

            for field_name in missing_fields:
                data = input(f"Ingrese un valor para {field_name}: ")
                self.results[field_name]["value"] = data
                self.results[field_name]["match"] = True
                self.results[field_name]["confidence"] = 100
    
    def display_all_values(self):
        for field_name, node in self.results.items():
            print(f"{field_name}: {node.get('value')}")









        

        
