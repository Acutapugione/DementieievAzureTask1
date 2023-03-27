import json
import os
import asyncio
from azure.storage.blob.aio import BlobServiceClient

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

# Асинхронная функция для скачивания файла или папки
async def download_blob(blob, container_client, local_path):
    # Получаем BlobClient для текущего Blob-объекта
    blob_client = container_client.get_blob_client(blob.name)

    # Локальный путь, куда будет скачан текущий Blob-объект
    local_file_path = f"{local_path}/{blob.name}"

    # Если текущий Blob-объект является папкой, создаем ее локально
    if '/' in blob.name:
        os.makedirs(local_file_path, exist_ok=True)
    # Если текущий Blob-объект является файлом, скачиваем его
    else:
        async with blob_client as download_blob:
            with open(local_file_path, "wb") as local_file:
                data = await download_blob.readall()
                local_file.write(data)

# Асинхронно получаем список всех Blob-объектов в контейнере
async def list_blobs(container_client, folder_path):
    async for blob in container_client.list_blobs(name_starts_with=folder_path):
        # Асинхронно скачиваем текущий Blob-объект
        await download_blob(blob, container_client, local_path)

# Запускаем цикл событий и асинхронно скачиваем все файлы и папки
async def main():
    await list_blobs(container_client, folder_path)

if __name__ == '__main__':
    asyncio.run(main())