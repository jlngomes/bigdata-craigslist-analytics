import kagglehub
import shutil
import os

# Download latest version
path = kagglehub.dataset_download("austinreese/craigslist-carstrucks-data")

print("Path to dataset files:", path)

destino_bronze = "../data/bronze"
if not os.path.exists(destino_bronze):
    os.makedirs(destino_bronze)

# 3. Move os arquivos baixados para a pasta bronze
for file in os.listdir(path):
    if file.endswith(".csv"):
        shutil.move(os.path.join(path, file), os.path.join(destino_bronze, file))
        print(f"Arquivo {file} movido para data/bronze!")