import operator
from typing import Annotated, Dict, List, TypedDict, Optional, Any
from langchain.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import BaseTool
from langchain.agents import create_agent # No AgentExecutor present. Is that a problem?
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
import json
import re
import os
from dotenv import load_dotenv
from logger import Logger
from input import DocumentHub

load_dotenv()

class InformationNode(TypedDict):
    match: bool
    value: Optional[str]
    explanation: Optional[str]
    confidence: Optional[int]


class DocumentCaptureState(TypedDict):
    documents: List[Dict[str, Any]] # List of documents 
    fields_to_extract: Dict[str, str]
    extracted_information: Dict[str, Dict[str, InformationNode]]  # This stores a dictionary that holds a result for each document
    results: Dict[str, InformationNode]
    iteration: int
    max_iterations: int
    confidence_high: bool
    sufficient_info: bool


class DocumentCaptureAgent:
    def __init__(self, llm, max_iterations: int = 5):
        self.llm = llm
        self.max_iterations = max_iterations
        self.logger = Logger()
        self.doc_hub = DocumentHub()
        self.graph = self._build_graph()
        

    def _build_graph(self) -> StateGraph:
        '''Build the graph according to definitions'''
        self.logger.info("Building graph...")
        
        workflow = StateGraph(DocumentCaptureState)

        # Add the nodes for all the defined states
        workflow.add_node("iterate_and_extract", self.iterate_and_extract)
        workflow.add_node("curate_and_disambiguate", self.curate_and_disambiguate)
        workflow.add_node("enough_confidence", self.enough_confidence)
        workflow.add_node("enough_information", self.enough_information)

        workflow.add_node("final_approval", self.final_approval)

        # Set the workflow entry point
        workflow.set_entry_point("iterate_and_extract")

        # Set the edges between nodes
        workflow.add_edge("iterate_and_extract", "curate_and_disambiguate")
        workflow.add_edge("curate_and_disambiguate", "enough_confidence")
        workflow.add_edge("enough_confidence", "enough_information")
        
        # This is probably not quite right
        workflow.add_edge("final_approval", END)

        
        # Add conditional edges
        # workflow.add_conditional_edges(
        #     "enough_confidence",
        #     self.enough_confidence,
        #     {
        #         "confident": "confirm_and_adjust",
        #         "not_confident": "offer_data_to_user"
        #     }
        # )

        workflow.add_conditional_edges(
            "enough_information",
            self.route_sufficient_info,
            {
                "sufficient": "final_approval",
                "not_sufficient": "iterate_and_extract"
            }
        )

        return workflow.compile()

    def iterate_and_extract(self, state: DocumentCaptureState) -> Dict[str, Any]:
        self.logger.info("---STATE: ITERATE AND EXTRACT---")

        documents = state["documents"]
        fields = state["fields_to_extract"]
        state["extracted_information"] = {}

        for document in documents:
            document_text = document["content"]
            prompt = self.build_extraction_prompt(document_text, fields)

            response = self.llm.invoke(prompt)
            json_response = self.clean_and_parse_json(response.content)


            state["extracted_information"][document["id"]] = json_response


        return state
            

    def clean_and_parse_json(self, text: str):
    # Remove markdown fences like ```json ... ```
        cleaned = re.sub(r"```[a-zA-Z]*", "", text)   # remove ```json or ``` or ```xyz
        cleaned = cleaned.replace("```", "")          # remove closing fences
        cleaned = cleaned.strip()
        return json.loads(cleaned)
            


    def build_extraction_prompt(self, document, fields_dict):
        system_text = '''
        Eres un asistente de extracción de información.

        Vas a recibir 2 entradas:
        1) Un documento, que puede consistir en un text o la decodificación OCR del contenido de una imagen.
        2) Un diccionario de campos para extraer. Cada campo del diccionario contiene un nombre y la explicación de lo que hay que extraer.
        
        Por cada campo DEBES determinar si el documento contiene o no la información buscada.

        Retorna un diccionario JSON cuyas claves son nombres de campo y cuyos valores son objetos con esta estructura:
        {
            "match": boolean,
            "value": string | null,
            "explanation": string | null,
            "confidence": int (0-100) | null
        }

        Rules:
        - match=true only if the field is **explicitly present** in the document.
        - If match=false, then set value=null, explanation=null, confidence=null.
        - explanation must reference **where** or **how** the model inferred the value
        (e.g., “Found in line about business owner: ‘Razon social:…’”).
        - confidence is 0–100. Use higher confidence when text is direct and explicit.
        - If inferred but not explicit, match=true but confidence must be <70 and explanation must state inference.
        - DO NOT hallucinate values not suggested in the text.
        - Answer only JSON. No prose outside JSON.
        '''

        user_text = f'''
        DOCUMENT:
        {document}

        FIELDS TO EXTRACT:
        {json.dumps(fields_dict, indent=2)}
        '''

        # content_blocks = []

        # # Convert the document into multi modal blocks
        # # First case, document is PDF (text)
        # if isinstance(document, str):
        #     content_blocks.append({
        #         "type": "text",
        #         "text": document
        #     })

        # elif isinstance(document, list):
        #     for part in document:
        #         if "text" in part:
        #             content_blocks.append({
        #                 "type": "text",
        #                 "text": part["text"]
        #             })

        #         elif "inLineData" in part:
        #             img = part["inLineData"]
        #             content_blocks.append({
        #                 "type": "image",
        #                 "source": {
        #                     "type": "base64",
        #                     "media_type": img["mimeType"],
        #                     "data": img["data"]
        #                 }
        #             })
                   

        # content_blocks.append({
        #     "type": "text",
        #     "text": f"FIELDS TO EXTRACT:\n{json.dumps(fields_dict, indent=2)}"
        # })        

        return [
            SystemMessage(system_text),
            HumanMessage(user_text)
        ]


    def curate_and_disambiguate(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''Handles analysis of captured information to disambiguate if needed'''
        self.logger.info("---STATE: CURATE AND DISAMBIGUATE---")
        # TODO: Implement logic to review `extracted_data` for consistency,
        # merge partial results, or resolve ambiguities. This could be another LLM call.

        extracted = state.get("extracted_information", {})
        fields = state.get("fields_to_extract", {})
        results = {}

        # Iterate over all fields to extract
        for field_name in fields.keys():
            hits = []

            # Collect hits across documents
            for doc_id, doc_fields in extracted.items():
                field_data = doc_fields.get(field_name)
                if field_data and field_data.get("match"):
                    hits.append(field_data)

            # If no match anywhere
            if not hits:
                results[field_name] = {
                    "match": False,
                    "value": None,
                    "explanation": None,
                    "confidence": None
                }
                continue

            # If there were hits, find out how many
            # Start with only one hit
            if len(hits) == 1:
                results[field_name] = hits[0]
                continue

            # More than one hit, check if they are all equal or different
            unique_values = { h.get("value") for h in hits }

            # If only one distinct value found (all equal)
            if len(unique_values) == 1:
                value = hits[0].get("value")

                # Average confidences
                confidences = [ h.get("confidence") for h in hits ]
                average_confidence = sum(confidences) / len(confidences)

                # Merge explanations
                explanations = [ h.get("explanation") for h in hits ]

                results[field_name] = {
                    "match": True,
                    "value": value,
                    "explanation": ", ".join(explanations),
                    "confidence": int(average_confidence)
                }

            else:
                # There were different values
                confidences = [ h.get("confidence") for h in hits ]
                min_confidence = min(confidences)

                # Store all conflicting values
                results[field_name] = {
                    "match": True,
                    "value": [ h.get("value") for h in hits ],
                    "explanation": [ h.get("explanation") for h in hits ], 
                    "confidence": int(min_confidence)
                }

        state["results"] = results
        return state


    
    def enough_confidence(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''Should prompt user if confidence is low'''
        self.logger.info("---STATE: ENOUGH CONFIDENCE---")

        low_confidence = self.find_low_confidence_fields(state)

        if not low_confidence:
            print("Ningún campo con poca confianza...")
            return state
        
        for item in low_confidence:
            field = item["field"]
            value = item["value"]
            confidence = item["confidence"]

            print(f"Campo: {field}, Valor: {value}, Confianza: {confidence}")

            new_value = input("Confirmar el valor (ENTER) o ingresar uno nuevo")

            if new_value.strip():
                state["results"][field]["value"] = new_value.strip()

            state["results"][field]["confidence"] = 100
           
        return state

    

    def find_low_confidence_fields(self, state, threshold: int = 80):
        low = []

        for field, result in state["results"].items():
            if not result:
                continue

            value = result.get("value")
            confidence = result.get("confidence")

            if confidence and confidence < threshold:
                low.append({
                    "field": field,
                    "value": value,
                    "confidence": confidence
                })

        return low
    

    def confirm_or_adjust(self, state: DocumentCaptureState) -> DocumentCaptureState:
        self.logger.info("---STATE: CONFIRM OR ADJUST---")
        # This node is for low-confidence extractions. It could trigger a
        # human-in-the-loop review. In a real scenario, you might use
        # `interrupt_before` when compiling the graph to pause execution here.
        self.logger.warn("Confidence is low. Looping back to extraction after adjustment (if any).")
        return state

    def enough_information(self, state: DocumentCaptureState) -> DocumentCaptureState:
        self.logger.info("---STATE: ENOUGH INFORMATION---")

        missing = []
        state["iteration"] += 1

        for field_name, field_info in state["results"].items():
            if not field_info.get("match"):
                missing.append(field_name)

        if missing:
            state["sufficient_info"] = False
        
        else:
            state["sufficient_info"] = True

        return state
    

    def route_sufficient_info(self, state: DocumentCaptureState) -> str:
        self.logger.info("---STATE: ROUTE SUFFICIENT INFO---")
        is_sufficient = "sufficient" if state["sufficient_info"] else "not_sufficient"
        return is_sufficient
        
    



    def final_approval(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''This is where the user signs off on the data'''
        self.logger.info("---STATE: FINAL APPROVAL---")

        results = state["results"]

        print("Lista de datos obtenidos:")
        for field, result in results.items():
            print(f"{field}: {result}")



        return state
    

    def offer_data_to_user(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''This is were we offer a lower confident option to the user for clarification'''
        self.logger.info("---STATE: OFFER DATA TO USER---")
        return state
    

    def do_capture(self, initial_state: DocumentCaptureState) -> DocumentCaptureState:
        final_state = self.graph.invoke(initial_state)
        return final_state
    

    def prepare_initial_state(self) -> DocumentCaptureState:
        '''
        This is the method that reads the source files and formats them.
        Then it adds them to an initial graph state.
        '''

        SOURCES_DIR = os.getenv("BUSINESS_INFO_FOLDER", "sources")

        # Generate document list to read the necessary files
        document_list: List[Dict[str, Any]] = []

        self.logger.info("Preparando el estado inicial para el agente...")

        self.doc_hub.load_documents()
        document_list = self.doc_hub.document_list


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

        self.logger.info("Estado inicial construido correctamente...")

        return initial_state
    
    


    
  

    

        