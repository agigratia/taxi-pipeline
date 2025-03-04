import os
import subprocess

def run_extract():
    print("\nMenjalankan Extract...")
    subprocess.run(["python", "extract.py"])

def run_transform():
    print("\nMenjalankan Transform...")
    subprocess.run(["python", "transform.py"])

def run_load():
    print("\nMenjalankan Load...")
    subprocess.run(["python", "load.py"])

def run_full_pipeline():
    print("\nMenjalankan Full Pipeline: Extract -> Transform -> Load")
    run_extract()
    run_transform()
    run_load()

def show_menu():
    while True:
        print("\n===== Data Pipeline CLI Menu =====")
        print("1. Extract Data")
        print("2. Transform Data")
        print("3. Load Data")
        print("4. Jalankan Semua (Extract -> Transform -> Load)")
        print("5. Keluar")
        
        choice = input("Pilih opsi (1-5): ")
        
        if choice == "1":
            run_extract()
        elif choice == "2":
            run_transform()
        elif choice == "3":
            run_load()
        elif choice == "4":
            run_full_pipeline()
        elif choice == "5":
            print("\nKeluar dari program.")
            break
        else:
            print("\nPilihan tidak valid, silakan coba lagi.")

if __name__ == "__main__":
    show_menu()
