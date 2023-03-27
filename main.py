import json
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

with open('.secrets.json') as _:
    secrets = json.load(fp=_).get("Azure")

# Ключ доступа к Blob Storage
connection_string = secrets.get("connection_string")
# Название контейнера, из которого нужно скачать файлы
container_name = secrets.get("container_name")
# Путь к папке, которую нужно скачать
folder_path = secrets.get("folder_path")
# Локальный путь, куда будут скачаны файлы и папки
local_path = secrets.get("local_path")

# Создаем объект BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Получаем объект ContainerClient
container_client = blob_service_client.get_container_client(container_name)

# Получаем список всех Blob-объектов в контейнере
blobs = container_client.list_blobs(name_starts_with=folder_path)

# Проходимся по списку Blob-объектов
for blob in blobs:
    # Получаем BlobClient для текущего Blob-объекта
    blob_client = container_client.get_blob_client(blob.name)

    # Локальный путь, куда будет скачан текущий Blob-объект
    local_file_path = f"{local_path}/{blob.name}"

    if '/' in blob.name:
        os.makedirs(local_file_path, exist_ok=True)
    else:
        with open(local_file_path, "wb") as local_file:
            download_stream = blob_client.download_blob()
            local_file.write(download_stream.readall())