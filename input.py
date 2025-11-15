import os
import base64
import io
import glob
from typing import List, Dict, Any, Optional
from pypdf import PdfReader
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from google import genai
from logger import Logger
from utils import TimeMeasure
from dotenv import load_dotenv

load_dotenv()
timer = TimeMeasure()

API_KEY = os.getenv("LL_API_KEY")
MODEL = os.getenv("LLM_MODEL")

class DocumentHub:
    def __init__(self, client):
        self.logger = Logger()
        self.source_folder = os.getenv("BUSINESS_INFO_FOLDER", "sources")
        self.document_list: List[Dict[str, Any]] = []

        # Add the Gemini client for file upload
        self.client = client


    # def load_documents(self):
    #     self.logger.info(f"Iniciando análisis de archivos desde ruta {self.source_folder}")

    #     for file_path in glob.glob(os.path.join(self.source_folder, "*")):

    #         # Check if file had already been loaded before. If so, ignore
    #         if any(doc["filename"] == os.path.basename(file_path) for doc in self.document_list):
    #             self.logger.info(f"Archivo {file_path} ya fue procesado anteriormente... ignorando...")
    #             continue

    #         self.logger.info(f"Procesando archivo {file_path}")
    #         document: Optional[Dict[str, Any]] = None
    #         try:
    #             if file_path.lower().endswith(".pdf"):
    #                 self.logger.info(f"Archivo {file_path} es PDF, leyendo texto...")
    #                 document = self._process_pdf_document(file_path)
    #             elif file_path.lower().endswith((".jpg", ".jpeg", ".png")):
    #                 self.logger.info(f"Archivo {file_path} es imagen, extrayendo texto por OCR...")
    #                 document = self._process_image_document(file_path)
    #             else:
    #                 self.logger.warning(f"Tipo de archivo no soportado: {file_path}")
    #                 continue

    #             if document:
    #                 self.logger.info("Extracción de texto exitosa...")
    #                 self.document_list.append(document)

    #         except Exception as e:
    #             self.logger.error(f"Error procesando archivo {file_path}: {e}")
    #             continue

    def load_documents(self):
        self.logger.info(f"Iniciando análisis de archivos desde ruta {self.source_folder}")

        for file_path in glob.glob(os.path.join(self.source_folder, "*")):

            # Capture the filename in question
            filename = os.path.basename(file_path)

            # Check if file had already been loaded before. If so, ignore
            if any(doc["filename"] == filename for doc in self.document_list):
                self.logger.info(f"Archivo {filename} ya fue procesado anteriormente... ignorando...")
                continue

            self.logger.info(f"Procesando archivo {file_path}")
            document: Optional[Dict[str, Any]] = None

            try:
                # Use time measurement
                upload_id = timer.start_measurement()

                # Upload file to Gemini client
                uploaded = self.client.files.upload(file=file_path)
                gemini_name = uploaded.name

                # End time measurement
                time_result = timer.report_time_elapsed(upload_id, "subida de archivo")
                self.logger.info(time_result)
                

                if file_path.lower().endswith(".pdf"):
                    self.logger.info(f"Archivo {file_path} es PDF")
                    document = self._create_document_entry("pdf", filename, gemini_name)

                elif file_path.lower().endswith((".jpg", ".jpeg", ".png")):
                    self.logger.info(f"Archivo {file_path} es imagen")
                    document = self._create_document_entry("image", filename, gemini_name)

                else:
                    self.logger.warning(f"Tipo de archivo no soportado: {file_path}")
                    continue

                if document:
                    self.logger.info("Documento cargado al cliente con éxito...")
                    self.document_list.append(document)

            except Exception as e:
                self.logger.error(f"Error procesando archivo {file_path}: {e}")
                continue


    def _create_document_entry(self, type: str, filename: str, gemini_name: str):
        document_entry = {
            "id": f"doc_{filename}",
            "filename": filename,
            "reference": gemini_name, 
            "processed_state": "pending",
            "type": type
        }

        return document_entry

    def _process_pdf_document(self, pdf_path: str) -> Optional[Dict[str, Any]]:
        '''Returns the representation of a PDF document for processing'''
        try:
            # Extract text from PDF file
            pdf_text = self.pdf_to_text(pdf_path)

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
        

    def _process_image_document(self, image_path: str) -> Optional[Dict[str, Any]]:
        '''Returns the representation of an image document for processing'''
        try:
            # Extract info from image using OCR
            ocr_text  = self.ocr_base64_image(image_path)
            
            # If we could extract the text, we process the document info
            if ocr_text:
                image_document = {
                    "id": f"image_document_1{os.path.basename(image_path)}",
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
        

    def pdf_to_text(self, file_path: str) -> str:
        '''Reads the content of a PDF file and returns it as text'''
        try:
            reader = PdfReader(file_path)
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
            return text

        except Exception as e:
            self.logger.error(f"Error reading PDF: {e}")
            return ""
        

    def ocr_base64_image(self, file_path, langs="spa+eng"):
        # Read image file
        with open(file_path, "rb") as image_file:
            img_bytes = image_file.read()
        
        img = Image.open(io.BytesIO(img_bytes))

        # Preprocessing in case quality is poor
        img = img.convert("L")  # Convert to grayscale
        img = ImageOps.invert(img)  # Invert colors
        img = img.filter(ImageFilter.MedianFilter())   # Remove noise

        text = pytesseract.image_to_string(img, lang=langs)
        return text 

    
