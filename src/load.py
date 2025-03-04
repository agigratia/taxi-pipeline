import pandas as pd
import os
import logging
from tabulate import tabulate

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Loader:
    def __init__(self, result_dir="result", lookup_file="data/taxi_zone_lookup.csv"):
        self.result_dir = result_dir
        self.lookup_file = lookup_file
        self.final_file_csv = os.path.join(result_dir, "final_data.csv")
        self.final_file_excel = os.path.join(result_dir, "final_data.xlsx")
        self.lookup_df = pd.read_csv(self.lookup_file) if os.path.exists(self.lookup_file) else None
    
    def load_data(self):
        """Menggabungkan seluruh file hasil transformasi dari result/ menjadi satu dataset"""
        if os.path.exists(self.final_file_csv) or os.path.exists(self.final_file_excel):
            logging.info("File final_data.csv atau final_data.xlsx sudah ada. Proses load dibatalkan.")
            return None
        
        all_files = [os.path.join(self.result_dir, f) for f in os.listdir(self.result_dir) if f.endswith(".csv")]
        
        if not all_files:
            logging.warning("Tidak ada file untuk diproses di result/")
            return None
        
        dataframes = []
        for file in all_files:
            try:
                df = pd.read_csv(file)
                dataframes.append(df)
            except Exception as e:
                logging.error(f"Gagal membaca {file}: {e}")
        
        if not dataframes:
            logging.error("Tidak ada data yang berhasil dimuat.")
            return None
        
        final_df = pd.concat(dataframes, ignore_index=True)
        logging.info(f"Berhasil menggabungkan {len(all_files)} file menjadi satu dataset.")
        logging.info(f"Kolom yang tersedia di dataset: {list(final_df.columns)}")
        return final_df
    
    def display_summary(self, df):
        """Menampilkan ringkasan data hasil akhir dalam bentuk tabel yang lebih terstruktur"""
        travel_data = {
            "Total Perjalanan": [len(df)],
            "Rata-rata Jarak (km)": [df['trip_distance'].mean()],
            "Perjalanan Terpanjang (km)": [df['trip_distance'].max()]
        }
        duration_data = {
            "Total Durasi (menit)": [df['trip_durasi'].sum()],
            "Rata-rata Durasi (menit)": [df['trip_durasi'].mean()],
            "Perjalanan Durasi Terlama (menit)": [df['trip_durasi'].max()]
        }
        cost_data = {
            "Total Biaya ($)": [df['total_amount'].sum()],
            "Perjalanan Biaya Tertinggi ($)": [df['total_amount'].max()],
            "Rata-rata Total Biaya ($)": [df['total_amount'].mean()]
        }
        
        logging.info(f"\nData Perjalanan:\n{tabulate(pd.DataFrame(travel_data), headers='keys', tablefmt='grid')}")
        logging.info(f"\nData Durasi:\n{tabulate(pd.DataFrame(duration_data), headers='keys', tablefmt='grid')}")
        logging.info(f"\nData Biaya:\n{tabulate(pd.DataFrame(cost_data), headers='keys', tablefmt='grid')}")
        
        # Distribusi metode pembayaran
        payment_distribution = df['payment_type'].value_counts().reset_index()
        payment_distribution.columns = ['Metode Pembayaran', 'Jumlah']
        logging.info(f"\nDistribusi Metode Pembayaran:\n{tabulate(payment_distribution, headers='keys', tablefmt='grid')}")
        
        # 10 Lokasi Pickup Paling Populer dengan Borough & Zone
        if 'pu_location_id' in df.columns and self.lookup_df is not None:
            pickup_counts = df['pu_location_id'].value_counts().head(10).reset_index()
            pickup_counts.columns = ['Pickup Location ID', 'Jumlah']
            pickup_merged = pickup_counts.merge(self.lookup_df, left_on='Pickup Location ID', right_on='LocationID', how='left')[['Borough', 'Zone', 'Jumlah']]
            logging.info(f"\n10 Lokasi Pickup Paling Populer:\n{tabulate(pickup_merged, headers='keys', tablefmt='grid')}")
        else:
            logging.warning("Kolom 'pu_location_id' tidak ditemukan atau lookup file tidak tersedia.")
        
        # 10 Lokasi Dropoff Paling Populer dengan Borough & Zone
        if 'do_location_id' in df.columns and self.lookup_df is not None:
            dropoff_counts = df['do_location_id'].value_counts().head(10).reset_index()
            dropoff_counts.columns = ['Dropoff Location ID', 'Jumlah']
            dropoff_merged = dropoff_counts.merge(self.lookup_df, left_on='Dropoff Location ID', right_on='LocationID', how='left')[['Borough', 'Zone', 'Jumlah']]
            logging.info(f"\n10 Lokasi Dropoff Paling Populer:\n{tabulate(dropoff_merged, headers='keys', tablefmt='grid')}")
        else:
            logging.warning("Kolom 'do_location_id' tidak ditemukan atau lookup file tidak tersedia.")
    
    def save_final_data(self, df, file_format="csv"):
        """Menyimpan hasil akhir ke dalam format CSV atau Excel"""
        if file_format == "csv":
            df.to_csv(self.final_file_csv, index=False)
            logging.info(f"Hasil akhir disimpan dalam format CSV: {self.final_file_csv}")
        elif file_format == "excel":
            df.to_excel(self.final_file_excel, index=False)
            logging.info(f"Hasil akhir disimpan dalam format Excel: {self.final_file_excel}")
        else:
            logging.error("Format penyimpanan tidak valid. Gunakan 'csv' atau 'excel'.")

# Contoh penggunaan
if __name__ == "__main__":
    loader = Loader()
    final_df = loader.load_data()
    
    if final_df is not None:
        loader.display_summary(final_df)
        loader.save_final_data(final_df, file_format="csv")
