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
from dotenv import load_dotenv
from logger import Logger

load_dotenv()

class InformationNode(TypedDict):
    match: bool
    value: Optional[str]
    explanation: Optional[str]
    confidence: Optional[int]


class DocumentCaptureState(TypedDict):
    documents: List[Dict[str, Any]] # List of documents 
    fields_to_extract: Dict[str, str]
    extracted_information: Dict[str, InformationNode]
    iteration: int
    max_iterations: int
    confidence_high: bool
    sufficient_info: bool


class DocumentCaptureAgent:
    def __init__(self, llm, max_iterations: int = 5):
        self.llm = llm
        self.max_iterations = max_iterations
        self.logger = Logger()
        

    def _build_graph(self) -> StateGraph:
        '''Build the graph according to definitions'''
        self.logger.info("Building graph...")
        
        workflow = StateGraph(DocumentCaptureState)

        # Add the nodes for all the defined states
        workflow.add_node("iterate_and_extract", self.iterate_and_extract)
        workflow.add_node("curate_and_disambiguate", self.curate_and_disambiguate)
        workflow.add_node("enough_confidence", self.enough_confidence)
        workflow.add_node("offer_data_to_user", self.offer_data_to_user)
        workflow.add_node("confirm_and_adjust", self.confirm_or_adjust)
        workflow.add_node("confirm_or_adjust", self.confirm_or_adjust)
        workflow.add_node("enough_information", self.enough_information)

        workflow.add_node("final_approval", self.final_approval)

        # Set the workflow entry point
        workflow.set_entry_point("iterate_and_extract")

        # Set the edges between nodes
        workflow.add_edges("iterate_and_extract", "curate_and_disambiguate")
        workflow.add_edges("curate_and_disambiguate", "enough_confidence")
        workflow.add_edges("offer_data_to_user", "confirm_and_adjust")
        workflow.add_edges("confirm_or_adjust", "enough_confidence")
        
        # This is probably not quite right
        workflow.add_edges("final_approval", END)

        
        # Add conditional edges
        workflow.add_conditional_edges(
            "enough_confidence",
            self.enough_confidence,
            {
                "confident": "confirm_and_adjust",
                "not_confident": "offer_data_to_user"
            }
        )

        workflow.add_conditional_edges(
            "enough_information",
            self.enough_information,
            {
                "sufficient": "final_approval",
                "not_sufficient": "iterate_and_extract"
            }
        )

        return workflow.compile()

    def iterate_and_extract(self, state: DocumentCaptureState) -> Dict[str, Any]:
        document = state["document"]
        questions = state["questions"]

        extraction_prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(
                    "You are an AI assistant tasked with extracting information from a document. "
                    "For each question, determine if the information is present in the document. "
                    "If present, extract the value, provide a brief explanation of where it was found, "
                    "and assign a confidence score (1-100). If not present, set 'match' to false, "
                    "and leave 'value', 'explanation', and 'confidence' as null."
                    "Return the results as a JSON object where keys are the questions."
                ),
                HumanMessage(
                    f"Document:\n{document}\n\nQuestions:\n{json.dumps(questions, indent=2)}\n\n"
                    "Please extract the information in JSON format:"
                ),
            ]
        )

        extractor = extraction_prompt | self.llm.bind(response_format={"type": "json_object"})
        response = extractor.invoke({"document": document, "questions": questions})


    def curate_and_disambiguate(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''Handles analysis of captured information to disambiguate if needed'''
        self.logger.info("---STATE: CURATE AND DISAMBIGUATE---")
        # TODO: Implement logic to review `extracted_data` for consistency,
        # merge partial results, or resolve ambiguities. This could be another LLM call.
        return state
    
    def enough_confidence(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''Should prompt user if confidence is low'''
        self.logger.info("---STATE: ENOUGH CONFIDENCE---")
        confidence_threshold = 80
        is_confident = True
        for field, data in state["extracted_data"].items():
            if data.get("match") and data.get("confidence", 0) < confidence_threshold:
                self.logger.warn(f"Low confidence for field '{field}' ({data.get('confidence')}%).")
                is_confident = False
                break
        state["confidence_is_high"] = is_confident
        return state
    

    def confirm_or_adjust(self, state: DocumentCaptureState) -> DocumentCaptureState:
        self.logger.info("---STATE: CONFIRM OR ADJUST---")
        # This node is for low-confidence extractions. It could trigger a
        # human-in-the-loop review. In a real scenario, you might use
        # `interrupt_before` when compiling the graph to pause execution here.
        self.logger.warn("Confidence is low. Looping back to extraction after adjustment (if any).")
        return state

    def enough_information(self, state: DocumentCaptureState) -> DocumentCaptureState:
        self.logger.info("---STATE: ENOUGH INFORMATION---")
        if state["iteration"] >= state["max_iterations"]:
            self.logger.warn("Max iterations reached.")
            state["information_is_sufficient"] = True
            return state

        is_sufficient = all(data.get("match") for data in state["extracted_data"].values())
        state["information_is_sufficient"] = is_sufficient
        return state

    def final_approval(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''This is where the user signs off on the data'''
        self.logger.info("---STATE: FINAL APPROVAL---")
        return state
    

    def offer_data_to_user(self, state: DocumentCaptureState) -> DocumentCaptureState:
        '''This is were we offer a lower confident option to the user for clarification'''
        self.logger.info("---STATE: OFFER DATA TO USER---")
        return state
    

        