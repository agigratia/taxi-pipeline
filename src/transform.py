# import pandas as pd
# import os
# import logging
# import re

# # Konfigurasi logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# class Transformer:
#     def __init__(self, staging_dir="staging", output_dir="result"):
#         self.staging_dir = staging_dir
#         self.output_dir = output_dir
#         os.makedirs(self.output_dir, exist_ok=True)
    
#     def normalize_column_names(self, df):
#         """Mengubah semua nama kolom menjadi snake_case"""
#         df.columns = [
#             re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower()  # Mengubah CamelCase menjadi snake_case
#             .replace(" ", "_")
#             .replace("-", "_")
#             for col in df.columns
#         ]
#         logging.info("Normalisasi nama kolom selesai.")
#         return df
    
#     def calculate_trip_duration(self, df):
#         """Menambahkan kolom trip_durasi dalam satuan menit"""
#         df['trip_durasi'] = (pd.to_datetime(df['lpep_dropoff_datetime']) - \
#                              pd.to_datetime(df['lpep_pickup_datetime'])).dt.total_seconds() / 60
#         logging.info("Kolom trip_durasi berhasil ditambahkan.")
#         return df
    
#     def convert_trip_distance(self, df):
#         """Mengubah trip_distance dari mil ke kilometer"""
#         df['trip_distance'] = df['trip_distance'] * 1.60934
#         logging.info("Konversi trip_distance ke kilometer selesai.")
#         return df
    
#     def map_payment_type(self, df):
#         """Mengubah kode payment_type menjadi label yang lebih mudah dipahami"""
#         payment_mapping = {
#             1: "Credit Card",
#             2: "Cash",
#             3: "No Charge",
#             4: "Dispute",
#             5: "Unknown",
#             6: "Voided Trip"
#         }
#         df['payment_type'] = df['payment_type'].map(payment_mapping).fillna("Unknown")
#         logging.info("Mapping payment_type selesai.")
#         return df
    
#     def transform(self):
#         """Melakukan semua proses transformasi pada file di staging dan menyimpannya ke result"""
#         for file in os.listdir(self.staging_dir):
#             file_path = os.path.join(self.staging_dir, file)
#             output_path = os.path.join(self.output_dir, file)
            
#             # Cek jika file hasil transformasi sudah ada
#             if os.path.exists(output_path):
#                 logging.info(f"File sudah diproses sebelumnya, dilewati: {output_path}")
#                 continue
            
#             if os.path.getsize(file_path) == 0:
#                 logging.warning(f"File kosong, dilewati: {file_path}")
#                 continue
            
#             try:
#                 df = pd.read_csv(file_path)
#                 if df.empty:
#                     logging.warning(f"DataFrame kosong setelah dibaca, dilewati: {file_path}")
#                     continue
#             except Exception as e:
#                 logging.error(f"Gagal membaca file {file_path}: {e}")
#                 continue
            
#             df = self.normalize_column_names(df)
#             df = self.calculate_trip_duration(df)
#             df = self.convert_trip_distance(df)
#             df = self.map_payment_type(df)
            
#             df.to_csv(output_path, index=False)
#             logging.info(f"File hasil transformasi disimpan: {output_path}")

# # Contoh penggunaan
# if __name__ == "__main__":
#     transformer = Transformer()
#     transformer.transform()

import pandas as pd
import os
import logging
import re

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transformer:
    def __init__(self, staging_dir="staging", output_dir="result"):
        self.staging_dir = staging_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def normalize_column_names(self, df):
        """Mengubah semua nama kolom menjadi snake_case dan memperbaiki nama yang salah"""
        df.columns = [
            re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower()  # Mengubah CamelCase menjadi snake_case
            .replace(" ", "_")
            .replace("-", "_")
            for col in df.columns
        ]
        
        # Perbaiki nama kolom yang berubah salah saat konversi
        rename_columns = {
            "p_u_location_i_d": "pu_location_id",
            "d_o_location_i_d": "do_location_id"
        }
        df.rename(columns=rename_columns, inplace=True)
        
        logging.info("Normalisasi nama kolom selesai.")
        return df
    
    def calculate_trip_duration(self, df):
        """Menambahkan kolom trip_durasi dalam satuan menit"""
        df['trip_durasi'] = (pd.to_datetime(df['lpep_dropoff_datetime']) - \
                             pd.to_datetime(df['lpep_pickup_datetime'])).dt.total_seconds() / 60
        logging.info("Kolom trip_durasi berhasil ditambahkan.")
        return df
    
    def convert_trip_distance(self, df):
        """Mengubah trip_distance dari mil ke kilometer"""
        df['trip_distance'] = df['trip_distance'] * 1.60934
        logging.info("Konversi trip_distance ke kilometer selesai.")
        return df
    
    def map_payment_type(self, df):
        """Mengubah kode payment_type menjadi label yang lebih mudah dipahami"""
        payment_mapping = {
            1: "Credit Card",
            2: "Cash",
            3: "No Charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided Trip"
        }
        df['payment_type'] = df['payment_type'].map(payment_mapping).fillna("Unknown")
        logging.info("Mapping payment_type selesai.")
        return df
    
    def transform(self):
        """Melakukan semua proses transformasi pada file di staging dan menyimpannya ke result"""
        for file in os.listdir(self.staging_dir):
            file_path = os.path.join(self.staging_dir, file)
            
            if os.path.getsize(file_path) == 0:
                logging.warning(f"File kosong, dilewati: {file_path}")
                continue
            
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    logging.warning(f"DataFrame kosong setelah dibaca, dilewati: {file_path}")
                    continue
            except Exception as e:
                logging.error(f"Gagal membaca file {file_path}: {e}")
                continue
            
            df = self.normalize_column_names(df)
            df = self.calculate_trip_duration(df)
            df = self.convert_trip_distance(df)
            df = self.map_payment_type(df)
            
            output_path = os.path.join(self.output_dir, file)
            df.to_csv(output_path, index=False)
            logging.info(f"File hasil transformasi disimpan: {output_path}")

# Contoh penggunaan
if __name__ == "__main__":
    transformer = Transformer()
    transformer.transform()
