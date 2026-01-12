from enum import Enum

from dotenv import load_dotenv
import os
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.embeddings.embeddings import Embeddings
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain_openai.embeddings import OpenAIEmbeddings
from pydantic import SecretStr

# Load environment variables
load_dotenv()


class Model(Enum):

    @property
    def key(self):
        if self == LanguageModel.GEMINI_2_5_FLASH or self == EmbeddingModel.GEMINI_EMBEDDING_001:
            return os.getenv("GOOGLE_API_KEY")
        elif self == LanguageModel.GLM_4_6 or self == EmbeddingModel.MISTRAL_EMBEDDING_5:
            return os.getenv("AQUEDUCT_API_KEY") 
        
class LanguageModel(Model, Enum):
    GEMINI_2_5_FLASH = "gemini-2.5-flash"
    GLM_4_6 = "glm-4.6-355b"

class EmbeddingModel(Model, Enum):
    GEMINI_EMBEDDING_001 = "models/gemini-embedding-001"
    MISTRAL_EMBEDDING_5 = "e5-mistral-7b"


def get_model_instance(model: Model) -> BaseChatModel | Embeddings:
    match model:
        case LanguageModel.GEMINI_2_5_FLASH:
            return ChatGoogleGenerativeAI(
                model=model.value, temperature=0.1, google_api_key=model.key
            )
        case LanguageModel.GLM_4_6:
            return ChatOpenAI(
            model=model.value,
            temperature=0.1,
            base_url="https://aqueduct.ai.datalab.tuwien.ac.at",
            api_key=SecretStr(model.key),
            timeout=295.0,
        )
        case EmbeddingModel.GEMINI_EMBEDDING_001:
            return GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001", google_api_key=model.key
        )
        case EmbeddingModel.MISTRAL_EMBEDDING_5:
            raise NotImplementedError("Mistral Embedding model not implemented yet.")
        case _:
            raise ValueError(f"Unsupported model: {model}")
