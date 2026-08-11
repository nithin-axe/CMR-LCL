import google.generativeai as genai
from flask import current_app

class GeminiClient:
    def __init__(self, api_key=None, model_name=None):
        self.api_key = api_key
        self.model_name = model_name
        self._model = None
        self._configured = False

    def _configure(self):
        if not self._configured:
            api_key = self.api_key or current_app.config.get("GEMINI_API_KEY")
            model_name = self.model_name or current_app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite")
            if not api_key:
                raise ValueError("GEMINI_API_KEY is not configured in environment or config.")
            genai.configure(api_key=api_key)
            self._model = genai.GenerativeModel(model_name)
            self._configured = True

    def generate(self, prompt: str) -> str:
        """Generate content from a text prompt using Gemini."""
        self._configure()
        response = self._model.generate_content(prompt)
        return response.text

    def generate_multimodal(self, prompt: str, parts=None, json_mode: bool = False) -> str:
        """Generate content from a text prompt plus optional binary parts (PDF/image bytes).

        ``parts`` is a list of ``(mime_type, data_bytes)`` tuples. Each is sent inline
        so scanned documents can be classified without a separate OCR step. Falls back
        to a text-only call when no parts are supplied.

        ``json_mode=True`` asks Gemini to constrain its output to valid JSON directly
        (supported since SDK 0.5+) instead of relying on prompt instructions alone -
        callers that then run the result through a regex-based JSON extractor were
        seeing occasional non-JSON output (extra prose, markdown fences, truncation)
        that had no diagnosable cause once collapsed into a generic parse failure.
        """
        self._configure()
        content = [prompt]
        for mime_type, data in (parts or []):
            if data:
                content.append({"mime_type": mime_type, "data": data})
        generation_config = {"response_mime_type": "application/json"} if json_mode else None
        response = self._model.generate_content(content, generation_config=generation_config)
        candidates = getattr(response, "candidates", None) or []
        if not candidates or not getattr(candidates[0], "content", None) or not candidates[0].content.parts:
            finish_reason = getattr(candidates[0], "finish_reason", None) if candidates else None
            block_reason = getattr(getattr(response, "prompt_feedback", None), "block_reason", None)
            raise ValueError(
                f"Gemini returned no usable content (finish_reason={finish_reason}, block_reason={block_reason})"
            )
        return response.text
