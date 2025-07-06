import os 
import pandas as pd
from google.cloud import storage 
from src.logger import logging
from src.custom_exception import CustomException
from config.path_config import RAW_DIR, CONFIG_PATH
from utils.common_functions import read_yaml

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"mlops-new-464504-ca3031e4f07e.json"

logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self,config):
        self.config = config['data_ingestion']
        self.bucket_name = self.config['bucket_name']
        self.file_names = self.config['bucket_file_names']  

        os.makedirs(RAW_DIR, exist_ok=True)
        logger.info(f"Data Ingestion started.....")

    def download_csv_from_gcp(self):
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            
            for file_name in self.file_names:
                file_path = os.path.join(RAW_DIR, file_name)

                if file_name=='animelist.csv':
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)
                    data = pd.read_csv(file_path,nrows=5000000)
                    data.to_csv(file_path, index=False)
                    logger.info(f"Large File {file_name} is  Downloaded to {file_path}")
                else:
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)
                    logger.info(f"File {file_name} is Downloaded to {file_path}")
            
        except Exception as e:
            logger.error(f"Error in downloading files from GCP: {e}")
            raise CustomException("Failed to download data",e) 
        
    def run(self):
        try:
            self.download_csv_from_gcp()
            logger.info("Data Ingestion completed successfully.")
        except CustomException as e:
            logger.error(f"Data Ingestion failed: {e}")
        finally:
            logger.info("Data Ingestion process finished.")


if __name__ == "__main__":
    config = read_yaml(CONFIG_PATH)
    data_ingestion = DataIngestion(config)
    data_ingestion.run()
