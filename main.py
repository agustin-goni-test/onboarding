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
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from logger import Logger
from dotenv import load_dotenv
from clients.kafka_producer import ConfluentProducerClient


load_dotenv()

logger = Logger()

def main():
    '''Main logic here'''

    logger.debug("We are in DEBUG mode...")

    # Create LLM for the agent
    llm = create_llm()
    
    # Create agent and call method to set up initial state
    agent = DocumentCaptureAgent(llm)
    # initial_graph_state = agent.prepare_initial_state()

    # Find the final state (invoke the agent)
    # final_state = agent.do_capture(initial_graph_state)

    # raw_data = final_state["results"]
    raw_data = json_result_mockup()
    print("\n\nFin de la inferencia...\n\n")

    manager = VolcadoManager(json.loads(raw_data))
    # manager.complete_results()
    manager.complete_results_mockup()
    manager.display_all_values()

    config = get_kafka_config()
    producer = ConfluentProducerClient(config)

    topic = "sop-af-ayc-firma"
    message_value = {
        "commerceRut": "96806110-0"
    }

    message_key = "comercio-96806110-0"

    producer.send_message(
        topic=topic,
        key=message_key,
        value=message_value
    )

    producer.close()


    message = manager.create_volcado_data()
    print(message.to_json(indent=4))


def get_kafka_config() -> Dict[str, Any]:
    kafka_producer_config: Dict[str, Any] = {
        'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '3DMA6VIPHQA7R2VA',
        'sasl.password': 'UtrhdrSmV8xq9nZligCU5GZpm+7lbn3GbIzrkoqErmtIg2WW16Qvu7wV/7Dd9+Vw',
        'client.id': 'CommerceProducer'
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
        "direccion_comercio": "Dirección principal del comercio",
        "correo_comercio": "Correo central de comunicaciones asociado al comercio",
        "telefono_comercio": "Teléfono central asociado al comercio",
        "nombre_contacto": "Nombre del contacto principal relacionado a la afiliación del comercio",
        "num_serie": "Número de serie del documento de identidad del contacto principal",
        "correo_contacto": "Dirección de email asociada al contacto principal",
        "telefono_contacto": "Número de teléfono asociado al contacto principal",
        "representante_legal": "Representante legal del comercio o sociedad",
        "constitucion": "Accionistas del comercio y porcentaje de la operación que tengan",
        "num_cuenta": "Número de cuenta identificada para el comercio",
        "tipo_cuenta": "Tipo de la cuenta declarada por el comercio",
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
                "content": [
                    {"text": "Extrae todos los términos buscados del siguiente texto."},
                    {"text": pdf_text} 
                ]
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
                "content": [
                    {"text": "Extrae todos los términos buscados del siguiente texto."},
                    {"text": ocr_text} 
                ]
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
