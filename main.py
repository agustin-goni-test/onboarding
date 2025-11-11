import os
import base64
import io
from typing import List, Dict, Any
from pypdf import PdfReader
from document_capture import DocumentCaptureState, DocumentCaptureAgent
from langchain_google_genai import ChatGoogleGenerativeAI
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from logger import Logger
from dotenv import load_dotenv

load_dotenv()

logger = Logger()

def main():
    if not os.path.exists("sources"):
        logger.error("No existe el directorio con los documentos...")
        return
    else:
        initial_graph_state = prepare_initial_state()
        print("OK")

    llm = create_llm()
    
    agent = DocumentCaptureAgent(llm)

    final_state = agent.do_capture(initial_graph_state)
    print(final_state)
        

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
    PDF_PATH = os.path.join(SOURCES_DIR, "document.pdf")
    IMAGE_PATH = os.path.join(SOURCES_DIR, "image.jpg")

    document_list: List[Dict[str, Any]] = []

    # Extract text from PDF file
    pdf_text = pdf_to_text(PDF_PATH)
    if pdf_text:
        pdf_document = {
            "id": "pdf_document_1",
            "filename": os.path.basename(PDF_PATH),
            "type": "pdf_text",
            "content": [
                {"text": "Extrae todos los términos buscados del siguiente texto."},
                {"text": pdf_text} 
            ]

        }

        # Add to document list        
        document_list.append(pdf_document)

    # Extract info from image
    ocr_text  = ocr_base64_image(IMAGE_PATH)
    if ocr_text:
        image_document = {
            "id": "image_document_1",
            "filename": os.path.basename(IMAGE_PATH),
            "type": "text",
            "content": [
                {"text": "Extrae todos los términos buscados del siguiente texto."},
                {"text": ocr_text} 
            ]
        }

        document_list.append(image_document)

        # I'm only including this so I don't forget. The ACTUAL fields will be part of the state
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
    
        


if __name__ == "__main__":
    main()



