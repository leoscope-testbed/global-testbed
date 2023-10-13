import os
import logging

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError


logger = logging.getLogger(__name__)


connection_string = "<Storage-account-connection-string>"
container = "leotest"
jobid = "test-job-id"
config_file = "/home/leotest/leotest-testbed-v2/leotest/experiment-config.yaml"


def upload_file(connection_string, container, local_path, remote_path, overwrite=True):
    """upload file to Azure Blob Storage"""

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    path_remove = os.path.join(
                    os.path.normpath(os.path.join(local_path, os.pardir)), 
                    '')
    # print('path_remove=', path_remove)

    file_path_azure = os.path.join(remote_path, 
                        local_path.replace(path_remove, ""))

    blob_client = blob_service_client.get_blob_client(
                                    container=container, 
                                    blob=file_path_azure)
    
    logging.info('uploading %s to %s' % (local_path, remote_path))
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=overwrite)


def download_file(connection_string, container, remote_path, local_path):
    """download file from Azure Blob Storage"""

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)

    with open(local_path, "wb") as download_file:
        download_file.write(container_client.download_blob(remote_path).readall())


def upload_folder(connection_string, container, local_path, remote_path, overwrite=True):
        """upload artifact folder to Azure Blob Storage"""

        blob_service_client = BlobServiceClient.from_connection_string(
                                                    connection_string)
        
        path_remove = os.path.join(
                        os.path.normpath(os.path.join(local_path, os.pardir)), 
                        '')

        for r,d,f in os.walk(local_path):       
            if f:
                for file in f:
                    # print('r=' + r + ' d=' + str(d) + ' f=' + str(f) + ' file=' + str(file))
                    file_path_azure = os.path.join(
                                            remote_path, 
                                            os.path.join(r,file)
                                            .replace(path_remove, ""))
                    file_path_local = os.path.join(r, file)
                    blob_client = blob_service_client.get_blob_client(
                                        container=container, 
                                        blob=file_path_azure)
                    # Upload the file
                    with open(file_path_local, "rb") as data:
                        blob_client.upload_blob(data, overwrite=overwrite)
            


upload_file(connection_string, container, config_file, "jobs/")
download_file(connection_string, container, 'jobs/experiment-config.yaml', "downloaded_config.yaml")

    # count = 0
    # while count < self.max_tries: 
    #     try: 
    #         for r,d,f in os.walk(local_path):       
    #             if f:
    #                 for file in f:
    #                     # print('r=' + r + ' d=' + str(d) + ' f=' + str(f) + ' file=' + str(file))
    #                     file_path_azure = os.path.join(
    #                                             remote_path, 
    #                                             os.path.join(r,file)
    #                                             .replace(path_remove, ""))
    #                     file_path_local = os.path.join(r, file)
    #                     blob_client = blob_service_client.get_blob_client(
    #                                         container=self.container, 
    #                                         blob=file_path_azure)
    #                     # Upload the file
    #                     with open(file_path_local, "rb") as data:
    #                         blob_client.upload_blob(data, overwrite=overwrite)
    #         break
    #     except ServiceRequestError: 
    #         count +=1
    #         logging.warn('error connecting to blob storage, attempt: %d' % (count))
