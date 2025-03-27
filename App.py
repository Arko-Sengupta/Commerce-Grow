import os
import logging
import requests
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Configure Logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.ERROR)
logging.basicConfig(level=logging.WARNING)

# Load Environment Variables
load_dotenv(".env")

class ProductCatalogApp:
    
    def __init__(self):
        self.df = None
        self.uploaded_file = None
        self.url = os.getenv("URL")
        self.data = {}
        
    def clean_data(self, data):
        if isinstance(data, dict):
            return {k: self.clean_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.clean_data(v) for v in data]
        elif isinstance(data, float) and (np.isnan(data) or np.isinf(data)):
            return None  # Replace NaN, Infinity with None
        else:
            return data
        
    def RequestAPI(self):
        try:
            response = requests.post(self.url, json=self.data)
            return response.json()
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        
    def Load_Data(self) -> None:
        try:
            self.uploaded_file = st.file_uploader("Upload XLSX file", type=['xlsx'])
            if self.uploaded_file:
                self.df = pd.read_excel(self.uploaded_file)
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
            
    def Display_Uploaded_Data(self) -> None:
        try:
            if self.df is not None:
               st.write("### Uploaded Data Preview")
               st.dataframe(self.df)
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        
    def Display_Expanded_Data(self):
        try:
            if self.df is not None and st.button("Expand Data"):
               st.write("### Expanded Data Preview")
               
               self.data = self.df.to_dict()
               self.data = self.clean_data(self.data)
               
               with st.spinner("Expanding Data..."):
                   self.data = self.RequestAPI()["response"]
               
               self.df = pd.DataFrame(self.data)
               st.dataframe(self.df)
               
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
            
    def run(self):
        st.title("Product Catalog Data Expansion")
        st.write("Upload an Excel file containing product catalog data to expand it.")
        
        self.Load_Data()
        self.Display_Uploaded_Data()
        self.Display_Expanded_Data()

if __name__ == "__main__":
    
    app = ProductCatalogApp()
    app.run()