import pandas as pd
import os
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "RatecodeID",
    "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount",
    "payment_type", "trip_type", "congestion_surcharge"
]

class Extractor:
    def __init__(self, input_dir="data", staging_dir="staging"):
        self.input_dir = input_dir
        self.staging_dir = staging_dir
        os.makedirs(self.staging_dir, exist_ok=True)
    
    def read_csv(self, file_path):
        """Membaca file CSV dan mengembalikan DataFrame"""
        try:
            df = pd.read_csv(file_path)
            df = self.ensure_schema(df)
            logging.info(f"CSV {file_path} berhasil diekstrak. {df.shape[0]} baris, {df.shape[1]} kolom.")
            return df
        except Exception as e:
            logging.error(f"Gagal membaca CSV {file_path}: {e}")
            return None
    
    def read_json(self, file_path):
        """Membaca file JSON dalam format array atau JSON Lines"""
        try:
            if os.path.getsize(file_path) == 0:
                raise ValueError("File JSON kosong")
            
            with open(file_path, 'r', encoding='utf-8') as file:
                first_char = file.read(1)
                if not first_char:
                    raise ValueError("File JSON kosong atau tidak memiliki format yang valid")
            
            try:
                df = pd.read_json(file_path, lines=True)
            except ValueError:
                df = pd.read_json(file_path)
            
            df = self.ensure_schema(df)
            logging.info(f"JSON {file_path} berhasil diekstrak. {df.shape[0]} baris, {df.shape[1]} kolom.")
            return df
        except Exception as e:
            logging.error(f"Gagal membaca JSON {file_path}: {e}")
            return None
    
    def ensure_schema(self, df):
        """Memastikan DataFrame memiliki semua kolom dalam schema"""
        for col in SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[SCHEMA_COLUMNS]
    
    def extract_and_store(self):
        """Mengekstrak semua file dalam folder data dan menyimpannya ke staging"""
        for root, _, files in os.walk(self.input_dir):
            for file in files:
                file_path = os.path.join(root, file)
                staging_path = os.path.join(self.staging_dir, file.replace(".json", ".csv"))
                
                if os.path.exists(staging_path):
                    logging.info(f"File sudah ada di staging, dilewati: {staging_path}")
                    continue
                
                if file.endswith(".csv"):
                    df = self.read_csv(file_path)
                elif file.endswith(".json"):
                    df = self.read_json(file_path)
                else:
                    continue
                
                if df is not None:
                    df.to_csv(staging_path, index=False)
                    logging.info(f"Data disimpan ke staging: {staging_path}")

# Contoh penggunaan
if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract_and_store()
