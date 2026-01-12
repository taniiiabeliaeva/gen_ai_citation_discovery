from enum import Enum

from dotenv import load_dotenv
import os
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from pydantic import SecretStr

# Load environment variables
load_dotenv()


class Model(Enum):
    GEMINI_2_5_FLASH = "gemini-2.5-flash"
    GLM_4_6 = "glm-4.6-355b"

    @property
    def key(self):
        if self == Model.GEMINI_2_5_FLASH:
            return os.getenv("GOOGLE_API_KEY")
        elif self == Model.GLM_4_6:
            return os.getenv("AQUEDUCT_API_KEY")


def get_model_instance(model: Model) -> BaseChatModel:
    if model == Model.GEMINI_2_5_FLASH:
        return ChatGoogleGenerativeAI(
            model=model.value, temperature=0.1, google_api_key=model.key
        )
    elif model == Model.GLM_4_6:
        return ChatOpenAI(
            model=model.value,
            temperature=0.1,
            base_url="https://aqueduct.ai.datalab.tuwien.ac.at",
            api_key=SecretStr(model.key),
            timeout=295.0,
        )
    else:
        raise ValueError(f"Unsupported model: {model}")
