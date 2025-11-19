import os
import base64
import io
import glob
import json
from typing import List, Dict, Any, Optional
from pypdf import PdfReader
from document_capture import DocumentCaptureState, DocumentCaptureAgent
from commerce_integration import VolcadoManager, EntidadesVolcado
from langchain_google_genai import ChatGoogleGenerativeAI
from google import genai
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from logger import Logger
from dotenv import load_dotenv
from clients.kafka_producer import ConfluentProducerClient
from utils import TimeMeasure



load_dotenv()

logger = Logger()
timer = TimeMeasure()

def main():
    '''Main logic here'''

    logger.debug("We are in DEBUG mode...")
    logger.info("\n-----EMPEZANDO PROCESO DE CAPTURA DE DATOS-------\n")
    id = timer.start_measurement()


    # Do inference process
    raw_data = run_inference_stage()
    logger.info("\n-----FIN DE PROCESO DE CAPTURA DE DATOS-------\n")
    
    ################################################################
    ################################################################
    # Acá estamos cambiando el RUT para poder hacer una prueba!!!
    ################################################################
    ################################################################

    logger.info("\n----AJUSTANDO RUT PARA PODER EJECUTAR UNA PRUEBA---\n")
    raw_data["rut_comercio"]["value"] = "78000292-1"

    ################################################################
    ################################################################
    # Eliminar esto!!!
    ################################################################
    ################################################################

    logger.info("\n----CREANDO OBJETO PARA VOLCADO----\n1")
    message_dict = create_integration_data(raw_data)
    # message_dict = get_test_message()

    # print(json.dumps(message_dict, indent=4))


    

    # # Create LLM for the agent and Gemini client
    # llm = create_llm()
    # client = genai.Client(api_key=os.getenv("LLM_API_KEY"))
    
    # # Create agent and call method to set up initial state
    # agent = DocumentCaptureAgent(llm, client, process_batch=True)
    # initial_graph_state = agent.prepare_initial_state()

    # # Find the final state (invoke the agent)
    # final_state = agent.do_capture(initial_graph_state)

    # raw_data = final_state["results"]
    # # raw_data = json_result_mockup()
    # print("\n\nFin de la inferencia...\n\n")
    # input("Presione una tecla para continuar...")


    # manager = VolcadoManager(raw_data)
    # manager.complete_results()
    # # manager.complete_results_mockup()
    # manager.display_all_values()

    # message = EntidadesVolcado()
    # message = manager.create_volcado_data()
    # print(message.to_json(indent=4))

    message = timer.calculate_time_elapsed(id)
    logger.info(message)

    logger.info("\n----ENVÍO DE MENSAJE A TÓPICO DE INTEGRACIÓN PARA VOLCAR----\n")
    success = send_message_to_topic(message_dict)

    message = timer.calculate_time_elapsed(id)
    logger.info(message)


    # config = get_kafka_config()
    # producer = ConfluentProducerClient(config)

    # topic = "sop-af-ayc-volcado-centrales-integracion"

    # # message_value = {
    # #     "commerceRut": "96806110-0"
    # # }

    # # file_path = "mockups/input.json"
    # # with open(file_path, 'r', encoding='utf-8') as f:
    # #     message_value = json.load(f)

    # message_key = "comercio-1112223334"

    # # message_dict = json.loads(message.to_json())


    # producer.send_message(
    #     topic=topic,
    #     key=message_key,
    #     value=message_dict
    # )

    # producer.close()

    # print(message_dict)


def run_inference_stage() -> Dict[str, Any]:
    llm = create_llm()
    client = genai.Client(api_key=os.getenv("LLM_API_KEY"))

    # Create agent and call method to set up initial state
    agent = DocumentCaptureAgent(llm, client, process_batch=False)
    initial_graph_state = agent.prepare_initial_state()

     # Find the final state (invoke the agent)
    final_state = agent.do_capture(initial_graph_state)

    raw_data = final_state["results"]
    print("\n\nFin de la inferencia...\n\n")
    input("Presione una tecla para continuar...\n\n")

    return raw_data


def create_integration_data(raw_data: Dict[str, Any]):
    manager = VolcadoManager(raw_data)
    manager.complete_results()
    # manager.complete_results_mockup()
    manager.display_all_values()

    message = EntidadesVolcado()
    message = manager.create_volcado_data()
    print(message.to_json(indent=4))

    file_path = "mockups/volcado.json"

    message_dict = json.loads(message.to_json())

    # Save to output for validation
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(message_dict, f, ensure_ascii=False, indent=4)

    print("\n\nArchivo de entidades de volcado grabado correctamente.\n\n")
    input("Presione una tecla para continuar...\n\n")

    return message_dict


def send_message_to_topic(message_dict: Dict[str, Any]):
    '''Method to send a message to the Kafka integration topic'''

    # Get configuration and create producer based on this configuration
    config = get_kafka_config()
    producer = ConfluentProducerClient(config)

    # Select the topic to send message to
    # topic = "sop-af-ayc-volcado-centrales-integracion"
    topic = os.getenv("TOPIC")

    # Use ID as part of the message key
    rut = message_dict["integrationCommerce"]["commerceRut"]
    message_key = f"comercio-{rut}"

    # Send message
    producer.send_message(
        topic=topic,
        key=message_key,
        value=message_dict
    )

    # Close producer
    producer.close()




def get_test_message():
    file_path = "mockups/volcado.json"
    with open(file_path, 'r', encoding='utf-8') as f:
        message_value = json.load(f)

    return message_value
    







def get_kafka_config(environment: str = "dev") -> Dict[str, Any]:
    '''Method to obtain the correct configuration for Confluent'''

    boostrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    security_protocol = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
    sasl_mechanism = os.getenv("SASL_MECHANISM", "PLAIN")

    if environment == "dev":
        sasl_username = os.getenv("SASL_USERNAME")
        sasl_password = os.getenv("SASL_PASSWORD")

    elif environment == "qa":
        sasl_username = os.getenv("SASL_USERNAME_QA")
        sasl_password = os.getenv("SASL_PASSWORD_QA")

    else:
        print("No está claro el ambiente al que debe apuntar la conexión del productor... abortando")
        return None

    client_id = os.getenv("CLIENT_ID")  

    # Create configuration based on environment
    kafka_producer_config: Dict[str, Any] = {
        'bootstrap.servers': boostrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanism': sasl_mechanism,
        'sasl.username': sasl_username,
        'sasl.password': sasl_password,
        'client.id': client_id
    }

    return kafka_producer_config

        

def create_llm() -> ChatGoogleGenerativeAI:
    api_key = os.getenv("LLM_API_KEY")
    model = os.getenv("LLM_MODEL")

    llm = ChatGoogleGenerativeAI(
        model=model,
        google_api_key=api_key,
        temperature=0    
    )
    
    return llm


def pdf_to_text(file_path: str) -> str:
    '''Reads the content of a PDF file and returns it as text'''
    try:
        reader = PdfReader(file_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text

    except Exception as e:
        logger.error(f"Error reading PDF: {e}")
        return ""
    

def read_image_and_encode(file_path: str) -> str:
    '''Reads an image file and returns its Base64 encoded string.'''
    with open(file_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')
    
def ocr_base64_image(file_path, langs="spa+eng"):
    # Decode base64
    with open(file_path, "rb") as image_file:
        img_bytes = image_file.read()
    
    img = Image.open(io.BytesIO(img_bytes))

    # Preprocessing in case quality is poor
    img = img.convert("L")  # Convert to grayscale
    img = ImageOps.invert(img)  # Invert colors
    img = img.filter(ImageFilter.MedianFilter())   # Remove noise

    text = pytesseract.image_to_string(img, lang=langs)
    return text 

def get_mime_type(filename: str) -> str:
    if filename.lower().endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    elif filename.lower().endswith(".png"):
        return "image/png"
    
    return "application/octet-stream"


def prepare_initial_state() -> DocumentCaptureState:
    '''
    This is the method that reads the source files and formats them.
    Then it adds them to an initial graph state.
    '''

    SOURCES_DIR = os.getenv("BUSINESS_INFO_FOLDER", "sources")

    # Generate document list to read the necessary files
    document_list: List[Dict[str, Any]] = []

    logger.info(f"Iniciando análisis de archivos desde ruta {SOURCES_DIR}")

    # Look for every file in the folder
    for file_path in glob.glob(os.path.join(SOURCES_DIR, "*")):
        logger.info(f"Procesando archivo {file_path}")
        document: Optional[Dict[str, Any]] = None
        try:
            if file_path.lower().endswith(".pdf"):
                logger.info(f"Archivo {file_path} es PDF, leyendo texto...")
                document = _process_pdf_document(file_path)
            elif file_path.lower().endswith((".jpg", ".jpeg", ".png")):
                logger.info(f"Archivo {file_path} es imagen, extrayendo texto por OCR...")
                document = _process_image_document(file_path)
            else:
                logger.warning(f"Tipo de archivo no soportado: {file_path}")
                continue

            if document:
                logger.info("Extracción de texto exitosa...")
                document_list.append(document)

        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}")
            continue


    # The list of fields to validate
    fields = {
        "rut_comercio": "El RUT que identifica la identidad del comercio o empresa que se afilia",
        "razon social": "Nombre legal o razón social del comercio, asociado al RUT registrado",
        "nombre_fantasía": "Nombre de fantasía por el que el comercio es conocido",
        "direccion_comercio": "Dirección principal asociada al comercio (no puede estar asociada a otra entidad o persona)",
        "correo_comercio": "Correo central de comunicaciones asociado al comercio",
        "telefono_comercio": "Teléfono central asociado al comercio",
        "nombre_contacto": "Nombre completo (nombres, todos los apellidos) 1del contacto principal relacionado a la afiliación del comercio",
        "num_serie": "Número de serie del documento de identidad del contacto principal",
        "correo_contacto": "Dirección de email asociada al contacto principal",
        "telefono_contacto": "Número de teléfono asociado al contacto principal",
        "representante_legal": "Representante legal del comercio o sociedad",
        "constitucion": "Accionistas del comercio y porcentaje de la operación que tengan",
        "num_cuenta": "Número de cuenta identificada para el comercio",
        "tipo_cuenta": "Tipo de la cuenta decla"
        "rada por el comercio",
        "banco": "Banco al que pertenece la cuenta encontrada para el comercio",
        "nombre_cuenta": "Nombre del titular de la cuenta. Si no existe, asumir que es el representante legal, con confianza de 50"
        # ... other fields
    }

    # Create the initial state and return it
    initial_state: DocumentCaptureState = {
        "documents": document_list,
        "fields_to_extract": fields,
        "extracted_information": {},
        "results": {},
        "iteration": 0,
        "max_iterations": 5,
        "confidence_high": False,
        "sufficient_info": False    
    }

    logger.info("Estado inicial construido correctamente...")

    return initial_state
    

def _process_pdf_document(pdf_path: str) -> Optional[Dict[str, Any]]:
    '''Returns the representation of a PDF document for processing'''
    try:
        # Extract text from PDF file
        pdf_text = pdf_to_text(pdf_path)

        # If text is available, build the document info and return it
        if pdf_text:
            pdf_document = {
                "id": f"pdf_document_{os.path.basename(pdf_path)}",
                "filename": os.path.basename(pdf_path),
                "processed_state": "pending",
                "type": "pdf_text",
                "content": pdf_text
            }
            return pdf_document
        
        # If no text could be extracted, raise exception
        raise Exception("No fue posible extraer el texto del PDF.")
    
    except Exception as e:
        raise Exception(f"El documento PDF no pudo ser procesado: {e}")
    

def _process_image_document(image_path: str) -> Optional[Dict[str, Any]]:
    '''Returns the representation of an image document for processing'''
    try:
        # Extract info from image using OCR
        ocr_text  = ocr_base64_image(image_path)
        
        # If we could extract the text, we process the document info
        if ocr_text:
            image_document = {
                "id": "image_document_1",
                "filename": os.path.basename(image_path),
                "processed_state": "pending",
                "type": "text",
                "content": ocr_text
            }
            return image_document
        
        # If not, we raise exception and return
        raise Exception("No fue posible extraer el texto de la imagen.")
    
    except Exception as e:
        raise Exception(f"El documento de imagen no pudo ser procesado: {e}")
    

def json_result_mockup() -> str:


    # Use this cleaned JSON version instead
    raw_data_json = '''
    {
        "rut_comercio": {
            "match": true,
            "value": "77.929.897-3",
            "explanation": "Found explicitly under 'Rut Sociedad' in the initial information block: 'Rut Sociedad: 77.929.897-3'",
            "confidence": 100
        },
        "razon_social": {
            "match": true,
            "value": "COMERCIAL LUMOS LIMITADA", 
            "explanation": "Found explicitly under 'Razón Social' in the initial information block",
            "confidence": 100
        },
        "nombre_fantasia": {
            "match": true,
            "value": "COMERCIAL LUMOS LIMITADA",
            "explanation": "Found explicitly in 'ARTÍCULO PRIMERO DEL NOMBRE O RAZON SOCIAL'",
            "confidence": 100
        },
        "direccion_comercio": {
            "match": true, 
            "value": "comuna de MAIPU, Región METROPOLITANA DE SANTIAGO",
            "explanation": "Found in 'ARTÍCULO TERCERO DOMICILIO'",
            "confidence": 100
        },
        "correo_comercio": {
            "match": false,
            "value": null,
            "explanation": null,
            "confidence": null,
            "has_conflict": false
        },
        "telefono_comercio": {
            "match": false,
            "value": null, 
            "explanation": null,
            "confidence": null,
            "has_conflict": false
        },
        "nombre_contacto": {
            "match": true,
            "value": "JUAN LU",
            "explanation": "Inferred as the principal contact because 'ARTÍCULO SÉPTIMO DE LA ADMINISTRACIÓN'",
            "confidence": 85
        },
        "num_serie": {
            "match": false,
            "value": null,
            "explanation": null, 
            "confidence": null,
            "has_conflict": false
        },
        "correo_contacto": {
            "match": false,
            "value": null,
            "explanation": null,
            "confidence": null,
            "has_conflict": false
        },
        "telefono_contacto": {
            "match": false,
            "value": null,
            "explanation": null,
            "confidence": null,
            "has_conflict": false
        },
        "representante_legal": {
            "match": true,
            "value": "JUAN LU", 
            "explanation": "Found in 'ARTÍCULO SÉPTIMO DE LA ADMINISTRACIÓN'",
            "confidence": 100
        },
        "constitucion": {
            "match": true,
            "value": "MENGQIANG LU 50%, JUAN LU 50%",
            "explanation": "Based on 'ARTÍCULO QUINTO DEL CAPITAL SOCIAL'",
            "confidence": 95
        },
        "num_cuenta": {
            "match": true,
            "value": "24031186",
            "explanation": "Found explicitly in the line 'Cuenta Corriente 24031186'",
            "confidence": 95
        },
        "tipo_cuenta": {
            "match": true,
            "value": "Corriente", 
            "explanation": "Found explicitly in the line 'Cuenta Corriente 24031186'",
            "confidence": 90
        },
        "banco": {
            "match": true,
            "value": "Bci",
            "explanation": "Inferred from 'Bci Preferencial' mentioned in the document",
            "confidence": 80
        },
        "nombre_cuenta": {
            "match": false,
            "value": null,
            "explanation": null,
            "confidence": null, 
            "has_conflict": false
        }
    }
    '''
    return raw_data_json




if __name__ == "__main__":
    main()
