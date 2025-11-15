from google import genai
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import os
from datetime import datetime
import json
import re

load_dotenv()

def clean_and_parse_json(text: str) -> str:
    # Remove markdown fences like ```json ... ```
    cleaned = re.sub(r"```[a-zA-Z]*", "", text)   # remove ```json or ``` or ```xyz
    cleaned = cleaned.replace("```", "")          # remove closing fences
    cleaned = cleaned.strip()
    return json.loads(cleaned)

api_key = os.getenv("LLM_API_KEY")
model = os.getenv("LLM_MODEL")

print(f"Modelo en uso para esta prueba: {model}..\n")
start_time = datetime.now()

client = genai.Client(api_key=api_key)

file = client.files.upload(file="sources/document.pdf")

time_after_upload = datetime.now()
time_lap = time_after_upload - start_time
total_lap_seconds = time_lap.total_seconds()
time_lap_minutes = int(total_lap_seconds // 60)
time_lap_seconds = int(total_lap_seconds % 60)
print(f"Tiempo para subir el archivo: {time_lap_minutes} minutos y {time_lap_seconds} segundos\n")


fields = {
            "rut_comercio": "El RUT que identifica el comercio o empresa que se afilia. DEBE contener guión (ejemplo '4.567.389-1' o '45768945-4')",
            "razon_social": "Nombre legal o razón social del comercio, asociado al RUT registrado",
            "nombre_fantasia": "Nombre de fantasía por el que el comercio es conocido",
            "direccion_comercio": "Dirección del comercio, con calle y número, y opcionalmente comuna y región (ej: 'Teatinos 500, Santiago, RM'). Si no hay calle o número la confianza es baja",
            "actividad_economica": "Actividad económica a la que se dedica la sociedad del comercio",
            "nombre_contacto": "Nombre del contacto principal relacionado a la afiliación del comercio",
            "rut_contacto": "RUT del contacto principal del comercio",
            "num_serie": "Número de serie del documento de identidad del contacto principal. Formato '111.111.111' o '111111111'. Puede contener letras pero NUNCA guiones",
            "correo_contacto": "Dirección de email asociada al contacto principal",
            "telefono_contacto": "Número de teléfono asociado al contacto principal",
            "representante_legal": "Representante legal del comercio o sociedad",
            "constitucion": "Accionistas del comercio y porcentaje de la operación que tengan",
            "num_cuenta": "Número de cuenta identificada para el comercio",
            "tipo_cuenta": "Tipo de la cuenta declarada por el comercio. Indicar sólo el tipo, omitir la palabra cuenta (ejemplo: 'ahorro' en vez de 'cuenta de ahorro' o 'cuenta ahorro')",
            "banco": "Banco al que pertenece la cuenta encontrada para el comercio",
            "nombre_cuenta": "Nombre del titular de la cuenta. Si no existe, asumir que es el representante legal, con confianza de 50"
            # ... other fields
        }


prompt=f'''
    Eres un asistente de extracción de información.

    Vas a recibir 2 entradas:
    1) Un documento, quue consiste en una imagen que captura información relevante
    2) Un diccionario de campos (field descriptions) para extraer. Cada campo del diccionario contiene un nombre y la explicación de lo que hay que extraer.

    Por cada campo DEBES determinar si el documento contiene o no la información buscada.

    Retorna un diccionario JSON cuyas claves son nombres de campo y cuyos valores son objetos con la estructura de abajo.
    DEBE existir un objeto con exactamente estas atributos en todos los casos, incluso para campos no encontrados:
    {{
        "match": boolean,
        "value": string | null,
        "explanation": string | null,
        "confidence": int (0-100) | null
    }}

    Rules:
    - match=true only if the field is **explicitly present** in the document.
    - If match=false, then set value=null, explanation=null, confidence=null.
    - explanation must reference **where** or **how** the model inferred the value
    (e.g., “Found in line about business owner: ‘Razon social:…’”).
    - confidence is 0–100. Use higher confidence when text is direct and explicit.
    - If a paramter is had "rut" in the name, express the value without '.' in it, no matter how it comes
    (e.g. if it is '10.345.678-2', express it as '10345678-2').
    -'num_serie' must also be expressed with no '.' in it (e.g., instead of '123.456.789', express it as '123456789').
    - If inferred but not explicit, match=true but confidence must be <70 and explanation must state inference.
    - DO NOT hallucinate values not suggested in the text.
    - If not all conditions for a value are present, confidence must be <70.
    - Answer only JSON. No prose outside JSON.

    Field descriptions:
    {fields}

'''

start_time = datetime.now()


response = client.models.generate_content(
    model = model,
    contents=[prompt, file]
)

output = clean_and_parse_json(response.candidates[0].content.parts[0].text)

# ensure_ascii = False to avoid JSON escaping of character such as "Ñ"
print(json.dumps(output, ensure_ascii=False, indent=2))

stop_time = datetime.now()
total_lap_seconds = (stop_time - start_time).total_seconds()
time_lap_minutes = int(total_lap_seconds // 60)
time_lap_seconds = int(total_lap_seconds % 60)
print(f"Tiempo para la inferencia: {time_lap_minutes} minutos y {time_lap_seconds} segundos\n")


print("Success!")


def clean_and_parse_json(text: str) -> str:
    # Remove markdown fences like ```json ... ```
    cleaned = re.sub(r"```[a-zA-Z]*", "", text)   # remove ```json or ``` or ```xyz
    cleaned = cleaned.replace("```", "")          # remove closing fences
    cleaned = cleaned.strip()
    return json.loads(cleaned)