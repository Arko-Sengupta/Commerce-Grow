import sys
import time
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
sys.stdout.reconfigure(encoding='utf-8')

# Configure Logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.ERROR)
logging.basicConfig(level=logging.WARNING)

# Setup WebDriver
options = webdriver.ChromeOptions()
# options.add_argument('--headless')
options.set_capability('timeouts', {'implicit': 10})

driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
driver.set_page_load_timeout(30)
driver.set_script_timeout(30)

class ETLPipeline:
    
    def __init__(self):
        self.Additional_Columns = [
            'Model Number',
            'Model Name',
            'Browse Type',
            'SIM Type',
            'Hybrid Sim Slot',
            'Touchscreen',
            'OTG Compatible',
            'Quick Charging',
            'Display Size',
            'Resolution',
            'Resolution Type',
            'GPU',
            'Display Type',
            'Display Colors',
            'Other Display Features',
            'Operating System',
            'Processor Brand',
            'Processor Type',
            'Processor Core',
            'Primary Clock Speed',
            'Primary Camera Available',
            'Primary Camera',
            'Primary Camera Features',
            'Secondary Camera Available',
            'Secondary Camera',
            'Secondary Camera Features',
            'Flash',
            'HD Recording',
            'Full HD Recording',
            'Video Recording',
            'Video Recording Resolution',
            'Dual Camera Lens',
            'Network Type',
            'Supported Networks',
            'Internet Connectivity',
            '3G',
            'GPRS',
            'Micro USB Port',
            'Bluetooth Support',
            'Bluetooth Version',
            'Wi-Fi',
            'Wi-Fi Version',
            'Wi-Fi Hotspot',
            'USB Connectivity',
            'Audio Jack',
            'Map Support',
            'GPS Support',
            'Touchscreen Type',
            'SIM Size',
            'Keypad Type',
            'User Interface',
            'Removable Battery',
            'SMS',
            'Sensors',
            'FM Radio',
            'FM Radio Recording',
            'Audio Formats',
            'Video Formats',
            'Battery Capacity',
            'Width',
            'Height',
            'Depth',
            'Weight'
            ]

    def search_string(self, mobile: str, color: str, storage: str) -> str:
        try:
            s, str_list = "", [mobile, color, storage]
        
            for feature in str_list:
                if feature != "":
                    s += "-" + str(feature).lower().strip().replace(" ", "-")
            return s[1:]
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        
    def ProductURLs(self, df: pd.DataFrame):
        try:
            product_urls = []
            
            for mobile, color, storage in zip(df['Mobile'], df['Colors'], df['Storage']):
                
                search_string = self.search_string(mobile, color, storage)
                url = f"https://www.flipkart.com/search?q={mobile.replace(" ", "%20")}"
                
                driver.get(url)
                time.sleep(2)

                soup = BeautifulSoup(driver.page_source, "html.parser")

                a_tags = soup.find_all("div", class_="cPHDOP")
                a_tags = [div.find("a") for div in a_tags if div.find("a") is not None]
                a_tags = [a["href"] for a in a_tags]
                
                product_url = None
                for p_url in a_tags:
                    if search_string in p_url:
                        product_url = "https://www.flipkart.com" + p_url
                
                product_urls.append(product_url)
            return product_urls
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        finally:
            driver.quit()
    
    def AddToDataFrame(self, url: str, df: pd.DataFrame, data: dict) -> pd.DataFrame:
        try:
            mask = df["Product URL"] == url
            
            for key, value in data.items():
                if key in df.columns:
                    df.loc[mask, key] = value
                else:
                    logging.warning(f"Column '{key}' not found in DataFrame, skipping update.")
    
            return df
        except Exception as e:
            logging.error("An Error Occurred: ", exc_info=True)
            raise e
            
    def run(self, df: pd.DataFrame):
        try:
            df[self.Additional_Columns] = None
            
            product_urls = self.ProductURLs(df)
            df["Product URL"] = product_urls
            
            for url in product_urls:
                if url is not None:                    
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")
                        
                        data = {}
                        table_tags = soup.find_all("table")
                        
                        for table in table_tags:
                            tds = table.find_all("td")
                            tds = [td.text for td in tds]
                            
                            if len(tds)%2 != 0:
                                tds = tds[:-1]
                                
                            for i in range(0, len(tds), 2):
                                data[tds[i]] = tds[i+1]
                         
                        delete_keys = []      
                        for key in data.keys():
                            if key not in self.Additional_Columns:
                                delete_keys.append(key)
                         
                        data = {k: v for k, v in data.items() if k not in delete_keys}
                    else:
                        data = {}
                
                df = self.AddToDataFrame(url=url, df=df, data=data)
            return df
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e