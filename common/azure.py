import os
import logging

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError

log = logging.getLogger(__name__)


def upload_file(connection_string, container, local_path, remote_path, overwrite=True):
    """upload file to Azure Blob Storage"""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    path_remove = os.path.join(
        os.path.normpath(os.path.join(local_path, os.pardir)), "")
    file_path_azure = os.path.join(remote_path, local_path.replace(path_remove, ""))

    blob_client = blob_service_client.get_blob_client(
        container=container, blob=file_path_azure)

    log.info("[azure] uploading local=%s → container=%s blob=%s",
             local_path, container, file_path_azure)
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=overwrite)
    log.info("[azure] upload complete local=%s → blob=%s", local_path, file_path_azure)


def download_file(connection_string, container, remote_path, local_path):
    """download file from Azure Blob Storage"""
    log.info("[azure] downloading container=%s blob=%s → local=%s",
             container, remote_path, local_path)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)
    with open(local_path, "wb") as download_file:
        download_file.write(container_client.download_blob(remote_path).readall())
    log.info("[azure] download complete blob=%s → local=%s", remote_path, local_path)


def upload_folder(connection_string, container, local_path, remote_path, overwrite=True):
    """upload artifact folder to Azure Blob Storage"""
    log.info("[azure] uploading folder local=%s → container=%s remote=%s",
             local_path, container, remote_path)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    path_remove = os.path.join(
        os.path.normpath(os.path.join(local_path, os.pardir)), "")
    uploaded = 0
    for r, d, f in os.walk(local_path):
        for file in f:
            file_path_azure = os.path.join(
                remote_path,
                os.path.join(r, file).replace(path_remove, ""))
            file_path_local = os.path.join(r, file)
            blob_client = blob_service_client.get_blob_client(
                container=container, blob=file_path_azure)
            log.debug("[azure] uploading file local=%s → blob=%s", file_path_local, file_path_azure)
            with open(file_path_local, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
            uploaded += 1
    log.info("[azure] folder upload complete local=%s files_uploaded=%d", local_path, uploaded)