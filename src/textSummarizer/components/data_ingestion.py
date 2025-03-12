import os
import requests
import urllib.request as request
import zipfile
from src.textSummarizer.logging import logger
from src.textSummarizer.utils.common import get_size
from pathlib import Path
from src.textSummarizer.entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self):
        if not os.path.exists(self.config.local_data_file):
            response = requests.get(self.config.source_url, stream=True)
            response.raise_for_status()  # Check if the request was successful

            with open(self.config.local_data_file, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            logger.info(f"{self.config.local_data_file} download: {response.headers}")
        else:
            logger.info(f"File already exits of size: {get_size(Path(self.config.local_data_file))}")

    def extract_zip_file(self):
        """
        Zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
            logger.info(f"Extracted files from {self.config.local_data_file} to {unzip_path}")
