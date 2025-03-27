import os
import json
import logging
import pandas as pd
from google import genai
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv(".env")

class GeminiLLM:

    def __init__(self) -> None:
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model = os.getenv("GEMINI_MODEL")
        
    def GeneratePrompt(self, data: dict) -> str:
        try:
            return f"""
                   Smartphone Data: {data}
                   Standardize the given smartphone data by following these rules:
               
                   1. **Brands, Models, Mobile, Model Name, Model Number**: Convert to title case (e.g., "iphone 11" → "iPhone 11").
                   2. **Colors**: Convert to capitalized format (e.g., "green" → "Green").
                   3. **Memory, Storage**: Ensure consistency in units (e.g., "4GB" → "4 GB").
                   4. **Camera, HD Recording, Full HD Recording, Video Recording**: Convert "Yes"/"No" to boolean (true/false).
                   5. **Rating**: Round to one decimal place (e.g., 4.628 → "4.6").
                   6. **Selling Price, Original Price**: Format as integers.
                   7. **SIM Type, Hybrid SIM Slot, Touchscreen, OTG Compatible, Quick Charging, Removable Battery**: Convert "Yes"/"No" to boolean.
                   8. **Display Size, Width, Height, Depth**: Ensure numeric values with units (e.g., "15.49 cm (6.1 inch)" → "15.49 cm").
                   9. **Resolution**: Standardize as "Width x Height Pixels".
                   10. **Processor Type, Operating System**: Convert to title case.
                   11. **Primary Camera, Secondary Camera**: Standardize format (e.g., "12MP + 12MP" → "12 MP + 12 MP").
                   12. **Network Type, Supported Networks, Internet Connectivity**: Convert to uppercase and use commas for separation.
                   13. **Bluetooth Version, Wi-Fi Version**: Maintain version numbers as-is.
                   14. **Audio Formats, Video Formats, Sensors**: Standardize comma-separated lists with consistent spacing.
                   15. **Weight**: Ensure numeric values followed by "g" (e.g., "194 g").
                   16. **Product URL**: Ensure it remains unchanged.
                   17. **Long Descriptions**: Compress lengthy descriptions into shorter summaries while retaining key details (e.g., camera features → "12MP dual camera with Night Mode, HDR, and 4K video support").
                   18. **Any Missing Values**: Replace `None` with `null`.
                   
                   Return the standardized data in JSON format, ensuring proper key-value pairs and no additional text.
                   """
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e

    def GeminiFlash(self, row_n: dict) -> dict:
        try:
            row_n = str(row_n)
            
            response = self.client.models.generate_content(
                model=self.model,
                contents=self.GeneratePrompt(row_n),
            )
            
            response = response.text[7:-3]
            try:
                return json.loads(response)
            except:
                return {}
        except Exception as e:
            logging.error("An Error Occurred: ", exc_info=e)
            raise e

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            for i in range(0, len(df)):
                
                row_n = df.iloc[i].to_json()
                row_n = self.GeminiFlash(row_n=row_n)
                
                df.iloc[i] = row_n
            
            return df
        except Exception as e:
            logging.error("An Error Occurred: ", exc_info=e)
            raise e