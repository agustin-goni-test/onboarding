from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from document_capture import InformationNode
from clients.bff_client import get_bff_cuenta_instance, get_bff_comercio_instance

# --- Sub-Component Classes ---

class IntegrationAddress(BaseModel):
    """Models the address structure used in Commerce and Branches."""
    region: int | None = None
    comune: int | None = None
    number: str | None = None
    fullAddress: List[str] | None = None
    addressWithoutNumber: str | None = None

class IntegrationName(BaseModel):
    names: str | None = None
    lastName: str | None = None
    secondLastName: str | None = None

class IntegrationTerminals(BaseModel):
    """Models the terminal configuration details."""
    commerceRut: str | None = None
    branchCode: int | None = None
    terminalId: Optional[str] = None # Assuming null means Optional[str]
    contractId: str | None = None
    technology: int | None = None
    ussdNumber: int | None = None
    user: str | None = None
    obs: str | None = None
    additionalInfo: str | None = None
    serviceId: int | None = None
    sellerRut: str | None = None
    terminalNumber: str | None = None
    configurationType: str | None = None

# --- Main Component Classes ---

class IntegrationCommerce(BaseModel):
    """Models the primary commerce integration details."""
    commerceRut: str | None = None
    businessName: str | None = None
    businessLine: int | None = None
    origin: str | None = None
    email: str | None = None
    emailPayment: str | None = None
    fantasyName: str | None = None
    name: str | None = None
    lastName: str | None = None
    mothersLastName: str | None = None
    mobilePhoneNumber: str | None = None
    sellerRut: str | None = None
    user: str | None = None
    integrationAddress: IntegrationAddress | None = None # Nested object
    obs: str | None = None

class IntegrationBankAccount(BaseModel):
    """Models the bank account details."""
    commerceRut: str | None = None
    bankCode: int | None = None
    ownerFullName: str | None = None
    ownerRut: str | None = None
    ownerEmail: str | None = None
    user: str | None = None
    accountType: int | None = None
    ownerAccountNumber: str | None = None
    serviceId: int | None = None
    paymentType: str | None = None

class IntegrationContact(BaseModel):
    """Models the integration contact details."""
    commerceRut: str | None = None
    legalRepresentative: bool | None = None
    names: str | None = None
    lastName: str | None = None
    secondLastName: str | None = None
    rut: str | None = None
    email: str | None = None
    phone: str | None = None
    serialNumber: str | None = None
    sign: bool | None = None
    third: bool | None = None
    user: str | None = None
    signAllowed: bool | None = None

class IntegrationBranches(BaseModel):
    """Models the branch details, which includes an address and terminals list."""
    branchId: Optional[str] = None
    mainBranch: bool | None = None
    branchVerticalId: int | None = None
    integrationAddress: IntegrationAddress | None = None  # Nested object
    businessName: str | None = None
    commerceRut: str | None = None
    email: str | None = None
    fantasyName: str | None = None
    description: str | None = None
    idMcc: int | None = None
    mobilePhoneNumber: str | None = None
    name: str | None = None
    webSite: str | None = None
    mantisaBill: str | None = None
    dvBill: str | None = None
    bankAccount: str | None = None
    mantisaHolder: str | None = None
    integrationType: str | None = None
    user: str | None = None
    emailContact: str | None = None
    merchantType: int | None = None
    commerceContactName: str | None = None
    commerceLegalRepresentativeName: str | None = None
    commerceLegalRepresentativeRut: str | None = None
    commerceLegalRepresentativePhone: str | None = None
    integrationTerminals: List[IntegrationTerminals]  | None = None # Nested list of objects

# --- Top-Level Class ---

class EntidadesVolcado(BaseModel):
    """The top-level object containing the entire JSON structure."""
    integrationCommerce: IntegrationCommerce | None = None
    integrationBankAccount: IntegrationBankAccount | None = None
    integrationContact: IntegrationContact | None = None
    integrationBranches: List[IntegrationBranches] | None = None

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

    
    def complete_results_mockup(self):
        '''Mockup method only used for quicker testing...'''
        missing_fields = ["correo_comercio",
                          "telefono_comercio",
                          "num_serie",
                          "correo_contacto",
                          "telefono_contacto",
                          "nombre_cuenta"
                          ]
        
        added_values = ["prueba@gmail.com",
                        "912345678",
                        "123123123",
                        "prueba@gmail.com",
                        "912345678",
                        "Juan Perez Soto"
                        ]
        
        if missing_fields:
            for field_name, added_value in zip(missing_fields, added_values):
                self.results[field_name]["value"] = added_value
                self.results[field_name]["match"] = True
                self.results[field_name]["confidence"] = 100


    def create_volcado_data(self) -> EntidadesVolcado:
        '''Creates the structure that will be sent to the integration topic'''
        # Create container object
        volcado = EntidadesVolcado()

        # Obtain RUT for the next steps
        rut = self._get_value("rut_comercio")


        # Create a commerce, bank account and contact
        commerce = self.get_integration_commerce_data()
        bank_account = self.get_integration_bank_account_data()
        contact = self.get_integration_contact_data()

        # Create one branch and assign to list of branches 
        branches = []
        branch = self.get_integration_branches_data()
        branches.append(branch)

        # Add every element to the container object
        volcado.integrationCommerce = commerce
        volcado.integrationBankAccount = bank_account
        volcado.integrationContact = contact
        volcado.integrationBranches = branches

        # Return the container object
        return volcado

    # PENDING: correct value for business line
    # PENDING: get value for the contact name (and correct the different possibilites of names)
    def get_integration_commerce_data(self) -> IntegrationCommerce:
        '''Create the main commerce information'''
        # Create the commerce object
        commerce = IntegrationCommerce()

        # Set the values according to the input - PENDING
        commerce.commerceRut = self._get_value("rut_comercio")
        commerce.businessName = self._get_value("razon_social")
        commerce.fantasyName = self._get_value("nombre_fantasia")
        commerce.businessLine = 0 # pending
        commerce.origin = "AUTOAFILIACION POS"
        commerce.email = self._get_value("correo_comercio")
        commerce.emailPayment = self._get_value("correo_contacto")

        # Obtain parts of the name (use contact name)
        integration_name = self._obtain_parts_of_name(self._get_value("nombre_contacto"))

        # Set name-related parameters
        commerce.name = integration_name.names
        commerce.lastName = integration_name.lastName
        commerce.mothersLastName = integration_name.secondLastName

        # Set fixed parameters
        commerce.sellerRut = "5-1"
        commerce.user = "AYC"
        
        # Create the address information. Move this to a new external method
        # Use helper method
        address_info = self._get_address_info(self._get_value("direccion_comercio"))
        
        # Add address to commerce info
        commerce.integrationAddress = address_info
        
        return commerce
    

    def _obtain_parts_of_name(self, name) -> IntegrationName:
        '''Helper method to separate a name'''
        # Split the input
        name_parts = name.split(" ")
        integration_name = IntegrationName()

        # Get the number of parts
        number_of_parts = len(name_parts)

        # If the name came in four or more parts (at least 2 names and 2 surnames)
        if number_of_parts >= 4:
            integration_name.secondLastName = name_parts[number_of_parts-1]
            integration_name.lastName = name_parts[number_of_parts-2]
            integration_name.names = " ".join(name_parts[:2])

        # If it came in 3 parts (assume 1 name and 2 surnames)
        elif number_of_parts == 3:
            integration_name.secondLastName = name_parts[2]
            integration_name.lastName = name_parts[1]
            integration_name.names = name_parts[0]

        # The name came in 2 parts (only name and surname)
        # We will repeat the surname just in case it's a problem
        elif number_of_parts == 2:
            integration_name.secondLastName = name_parts[1]
            integration_name.lastName = name_parts[1]
            integration_name.names = name_parts[0]

        # If there is only one part (i.e., "Sting") simply repeat in every case
        else:
            integration_name.secondLastName = name_parts[0]
            integration_name.lastName = name_parts[0]
            integration_name.names = name_parts[0]

        return integration_name




    def _get_address_info(self, address_value) -> IntegrationAddress:
        '''Helper method used to create the address object with a specific format
        from the address string obtained in the inference.'''
        # Create the address object
        address = IntegrationAddress()

        # Add the elements --- ALL OF THIS IS STILL PENDING
        address.region = 0 # pending
        address.comune = 0 # pending
        address.number = "" # pending
        address.fullAddress = [] # pending
        address.addressWithoutNumber = "" # pending

        # Return address object
        return address
    

    # PENDING: Will the account name be the same as the contact name?
    # PENDING: Rut de contacto
    def get_integration_bank_account_data(self) -> IntegrationBankAccount:
        '''Create the bank account information'''
        # Create bank account object
        bank_account = IntegrationBankAccount()

        # Get instance for BFF Cuenta
        bff_cuenta = get_bff_cuenta_instance()
        bff_cuenta.populate_account_data()
        account_type_code = bff_cuenta.obtain_account_type_code(self._get_value("tipo_cuenta"))
        bank_code = bff_cuenta.obtain_bank_code(self._get_value("banco"))

        # Set values from inference -- SOME PENDING
        bank_account.commerceRut = self._get_value("rut_comercio")
        bank_account.bankCode = bank_code
        bank_account.ownerFullName = self._get_value("nombre_contacto")
        bank_account.ownerRut = self._get_value("rut_contacto")  # pending  
        bank_account.ownerEmail = self._get_value("correo_contacto")
        bank_account.user = "AYC"
        bank_account.accountType = account_type_code
        bank_account.ownerAccountNumber = self._get_value("num_cuenta")

        # Return bank account object
        return bank_account
    
    def get_integration_contact_data(self) -> IntegrationContact:
        '''Create the contact information'''
        # Create contact object
        contact = IntegrationContact()
        
        # Set the values from inference input -- SOME VALIDATIONS NEEDED
        contact.commerceRut = self._get_value("rut_comercio")
        contact.legalRepresentative = False  # validate

         # Obtain parts of the name (use contact name)
        integration_name = self._obtain_parts_of_name(self._get_value("nombre_contacto"))

        # Set name-related parameters
        contact.names = integration_name.names
        contact.lastName = integration_name.lastName
        contact.secondLastName = integration_name.secondLastName

        # Remaining parameters
        contact.rut = self._get_value("rut_contacto") # pending
        contact.email = self._get_value("correo_contacto")
        contact.phone = self._get_value("telefono_contacto")
        contact.serialNumber = self._get_value("num_serie")
        contact.sign = True  # validate
        contact.third = False  # validate
        contact.user = "AYC"
        contact.signAllowed = False  # validate

        return contact

    # PENDING: Validar si merchantType es giro
    # PENDING: rut contacto
    # PENDING: actividad económica
    def get_integration_branches_data(self) -> IntegrationBranches:
        '''Create a single branch'''
        # Create branch object
        branches = IntegrationBranches()

        actividad = "FABRICACIÓN DE CABLES DE FIBRA ÓPTICA"

        # Get instance of BFF for economic activities
        bff_comercio = get_bff_comercio_instance()
        bff_comercio.populate_economic_activities()

        activity_code = bff_comercio.obtain_activity_code(actividad)
        bff_comercio.fetch_mcc_info(activity_code)
        mcc, giro = bff_comercio.get_giro_and_mcc(activity_code)

        # Add simple values from inference
        branches.branchId = None
        branches.mainBranch = True
        branches.branchVerticalId = 0 # Internal code, not really needed
        branches.businessName = self._get_value("razon_social")
        branches.commerceRut = self._get_value("rut_comercio")
        branches.email = self._get_value("correo_comercio")
        branches.fantasyName = self._get_value("nombre_fantasia")
        branches.description = ""
        branches.idMcc = mcc 
        branches.mobilePhoneNumber = self._get_value("telefono_comercio")
        branches.name = self._get_value("razon_social")
        branches.webSite = ""
        branches.mantisaBill = self._get_value("rut_comercio")
        branches.dvBill = (self._get_value("rut_comercio")).split("-")[1]
        branches.bankAccount = self._get_value("num_cuenta")
        branches.mantisaHolder = self._get_value("rut_comercio")
        branches.integrationType = "PRO" # fixed value that will work
        branches.user = "AYC"
        branches.emailContact = self._get_value("correo_contacto")
        branches.merchantType = mcc # Validate
        branches.commerceContactName = self._get_value("nombre_contacto")
        branches.commerceLegalRepresentativeName = self._get_value("nombre_contacto")
        branches.commerceLegalRepresentativeRut = self._get_value("rut_contacto") # DOES NOT EXIST!!
        branches.commerceLegalRepresentativePhone = self._get_value("telefono_contacto") 

        # Create address object and assign
        address_info = self._get_address_info(self._get_value("direccion_comercio"))
        branches.integrationAddress = address_info

        # Create a certain number of terminals (2 for this test)
        branches.integrationTerminals = self.add_integration_terminals(2)

        # Return branch object
        return branches
    

    def add_integration_terminals(self, requested_amount: int = 2) -> List[IntegrationTerminals]:
        '''Create a list of terminals'''
        # Create a list of terminals
        terminals = []

        # Keep count, start with 0
        count = 0

        # Add the specified number
        while count < requested_amount:
            terminal = IntegrationTerminals()
            terminal.commerceRut = self._get_value("rut_comercio")
            terminal.branchCode = 0
            terminal.terminalId = None
            terminal.contractId = "0"
            terminal.technology = 20
            terminal.ussdNumber = 0
            terminal.user = "AYC"
            terminal.obs = "-SIM: CLARO -MODELO: POSANDROIDMOVIL -CANAL: AUTOAFILIACION. CANAL_ORIGEN: AUTOAFILIACION_POS "
            terminal.additionalInfo = ""
            terminal.serviceId = 4
            terminal.sellerRut = "5-1"
            terminal.terminalNumber = "0"
            terminal.configurationType = "RED_POS"

            # Append to list and increment count
            terminals.append(terminal)
            count += 1

        # Return list of terminals
        return terminals

    



        







        









        

        
