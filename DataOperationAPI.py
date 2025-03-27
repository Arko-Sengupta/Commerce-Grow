import logging
import pandas as pd
from flask import Blueprint, Flask, jsonify, request
from Backend.ETLPipeline import ETLPipeline
from Backend.GeminiLLM import GeminiLLM

# Configure Logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.ERROR)
logging.basicConfig(level=logging.WARNING)

class DataOperation:
    
    def __init__(self) -> None:
        self.elt_pipeline = ETLPipeline()
        self.geminiLLM = GeminiLLM()
        
    def ConvertToDataFrame(self, data: dict) -> pd.DataFrame:
        try:
            return pd.DataFrame.from_dict(data)
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        
    def ExpandData(self, data: dict) -> pd.DataFrame:
        try:
            df = self.ConvertToDataFrame(data)
            df = self.elt_pipeline.run(df=df)
            df = self.geminiLLM.run(df=df)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logging.error("An Error Occured: ", exc_info=e)
            raise e
        
class DataOperation_API:
    
    def __init__(self) -> None:
        self.app = Flask(__name__)
        self.data_operation = DataOperation()
        self.data_operation_blueprint = Blueprint('data_operation', __name__)
        self.data_operation_blueprint.add_url_rule('/data_operation', 'data_operation', self.DataOperation, methods=['POST'])

    def DataOperation(self) -> tuple:
        try:
            data = request.get_json()
            
            response = self.data_operation.ExpandData(data=data)
            return jsonify({'response': response}), 200
        except Exception as e:
            logging.error('An error occurred: ', exc_info=e)
            return jsonify({'Error': str(e)}), 400

    def run(self) -> None:
        try:
            self.app.register_blueprint(self.data_operation_blueprint)
            self.app.run(debug=True)
        except Exception as e:
            logging.error('An error occurred: ', exc_info=e)
            raise e
        

server = DataOperation_API()
if __name__ == '__main__':

    server.run()