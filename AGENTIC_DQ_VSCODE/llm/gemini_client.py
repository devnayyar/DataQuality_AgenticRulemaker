# llm/gemini_client.py
import google.generativeai as genai

from config.settings import GEMINI_API_KEY
genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel('gemini-2.5-flash')