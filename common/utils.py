import os
import shutil
import grpc
import time
import logging
import requests
import datetime
import subprocess
import socket
import yaml
from pyroute2 import IPRoute
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, \
    ResourceTypes, AccountSasPermissions, generate_blob_sas

log = logging.getLogger(__name__)


def time_now():
    now = datetime.datetime.utcnow()
    return now

def read_yaml(file):
    with open(file) as f:
        try:
            file_info = yaml.safe_load(f)
        except yaml.YAMLError:
            log.exception("[utils] YAML parse error reading %s", file)
            raise
    return file_info

def get_public_ip():
    """Returns own public IP address."""
    info = read_yaml("ext_depen/ext_dependency.yaml")
    ip = requests.get(info["server"]["aws"]).text.strip()
    log.info("[utils] public IP: %s", ip)
    return str(ip)

def get_weather_mon_info():
   """Returns weather related information."""
   info = read_yaml('ext_depen/ext_dependency.yaml')
   api=info['systems']['weather']['api']
   lat=info['systems']['weather']['lat'] 
   lon=info['systems']['weather']['lon'] 
   ele=info['systems']['weather']['ele'] 
   return str(api), str(lat), str(lon), str(ele)

def get_satellite_info():
   """Returns satellite constellation information."""
   info = read_yaml('ext_depen/ext_dependency.yaml')
   sat_info = info['systems']['satellite']  
   return str(sat_info['tle_url']), str(sat_info['shells'])

def execute(name, *args, **kwargs):
    opts = [name]
    for arg in args:
        opts.append(arg)
    for key, val in kwargs.items():
        opts.append("-%s" % key)
        opts.append(val)
    log.debug("[utils][execute] cmd=%s", opts)
    starttime = time_now()
    output = subprocess.run(opts, text=True, capture_output=True)
    endtime = time_now()
    return starttime, endtime, output

def route(action, ip, gw, dev):
    with IPRoute() as ipr:
        try:
            ipr.route(action, dst=ip, gateway=gw, oif=ipr.link_lookup(ifname=dev))
        except Exception as e:
            log.warning("[utils][route] action=%s ip=%s gw=%s dev=%s error=%s",
                        action, ip, gw, dev, e)


class StorageDirectoryClient:
  """Class to handle interaction with cloud storage. 
  Currently implemented for Azure Blob Storage, for other storage services, function overload the methods defined."""
  def __init__(self, connection_string, container_name):
    service_client = BlobServiceClient.from_connection_string(
      connection_string,
      max_single_put_size=4*1024*1024
    )
    self.container_name = container_name
    self.connection_string = connection_string
    self.client = service_client.get_container_client(container_name)

  def upload(self, source, dest):
    '''
    Upload a file or directory to a path inside the container
    '''
    if (os.path.isdir(source)):
      self.upload_dir(source, dest)
    else:
      self.upload_file(source, dest)

  def upload_file(self, source, dest):
    '''
    Upload a single file to a path inside the container
    '''
    log.info("[storage] uploading %s → %s (container=%s)", source, dest, self.container_name)
    with open(source, "rb") as data:
        self.client.upload_blob(name=dest, data=data, connection_timeout=300)
    log.info("[storage] upload complete %s → %s", source, dest)

  def upload_dir(self, source, dest):
    '''
    Upload a directory to a path inside the container
    '''
    prefix = '' if dest == '' else dest + '/'
    prefix += os.path.basename(source) + '/'
    for root, dirs, files in os.walk(source):
      for name in files:
        dir_part = os.path.relpath(root, source)
        dir_part = '' if dir_part == '.' else dir_part + '/'
        file_path = os.path.join(root, name)
        blob_path = prefix + dir_part + name
        self.upload_file(file_path, blob_path)

  def download(self, source, dest):
    '''
    Download a file or directory to a path on the local filesystem
    '''
    if not dest:
      raise Exception('A destination must be provided')

    blobs = self.ls_files(source, recursive=True)
    if blobs:
      # if source is a directory, dest must also be a directory
      if not source == '' and not source.endswith('/'):
        source += '/'
      if not dest.endswith('/'):
        dest += '/'
      # append the directory name from source to the destination
      dest += os.path.basename(os.path.normpath(source)) + '/'

      blobs = [source + blob for blob in blobs]
      for blob in blobs:
        blob_dest = dest + os.path.relpath(blob, source)
        self.download_file(blob, blob_dest)
    else:
      self.download_file(source, dest)

  def download_file(self, source, dest):
    '''
    Download a single file to a path on the local filesystem
    '''
    log.info("[storage] downloading source=%s dest=%s", source, dest)
    if dest.endswith("."):
        dest += "/"
    blob_dest = dest + os.path.basename(source) if dest.endswith("/") else dest
    os.makedirs(os.path.dirname(blob_dest), exist_ok=True)
    log.info("[storage] downloading blob=%s → local=%s", source, blob_dest)
    bc = self.client.get_blob_client(blob=source)
    if not dest.endswith("/"):
        with open(blob_dest, "wb") as file:
            data = bc.download_blob()
            file.write(data.readall())
        log.info("[storage] download complete blob=%s → %s", source, blob_dest)

  def check_blob_exists(self, source):
    '''
    Check if a blob exists
    '''
    log.debug("[storage] check_blob_exists source=%s container=%s", source, self.container_name)
    bc = self.client.get_blob_client(blob=source)
    exists = bc.exists()
    log.debug("[storage] check_blob_exists source=%s exists=%s", source, exists)
    return exists

  def ls_files(self, path, recursive=False):
    '''
    List files under a path, optionally recursively
    '''
    if not path == '' and not path.endswith('/'):
      path += '/'

    blob_iter = self.client.list_blobs(name_starts_with=path)
    files = []
    for blob in blob_iter:
      relative_path = os.path.relpath(blob.name, path)
      if recursive or not '/' in relative_path:
        files.append(relative_path)
    return files

  def ls_dirs(self, path, recursive=False):
    '''
    List directories under a path, optionally recursively
    '''
    if not path == '' and not path.endswith('/'):
      path += '/'

    blob_iter = self.client.list_blobs(name_starts_with=path)
    dirs = []
    for blob in blob_iter:
      relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
      if relative_dir and (recursive or not '/' in relative_dir) and not relative_dir in dirs:
        dirs.append(relative_dir)

    return dirs

  def rm(self, path, recursive=False):
    '''
    Remove a single file, or remove a path recursively
    '''
    if recursive:
        self.rmdir(path)
    else:
        log.info("[storage] deleting blob path=%s", path)
        self.client.delete_blob(path)
        log.info("[storage] blob deleted path=%s", path)

  def rmdir(self, path):
    '''
    Remove a directory and its contents recursively
    '''
    blobs = self.ls_files(path, recursive=True)
    if not blobs:
        log.info("[storage] rmdir: no blobs found under path=%s — nothing to delete", path)
        return

    if not path == "" and not path.endswith("/"):
        path += "/"
    blobs = [path + blob for blob in blobs]
    log.info("[storage] deleting %d blobs under path=%s", len(blobs), path)
    self.client.delete_blobs(*blobs)
    log.info("[storage] rmdir complete path=%s deleted=%d", path, len(blobs))

  def get_sas_url(self, blob_name):
      
      blob_service_client = BlobServiceClient.from_connection_string(
         self.connection_string
      )
      account_name = blob_service_client.account_name
      sas_token = generate_blob_sas(
          account_name=account_name,
          account_key=blob_service_client.credential.account_key,
          container_name=self.container_name,
          blob_name=blob_name,
          permission=AccountSasPermissions(read=True),
          expiry=datetime.datetime.utcnow() + datetime.timedelta(days=300)
    )

      return "https://"+account_name+".blob.core.windows.net/"+self.container_name+"/"\
                                  +blob_name+"?"+sas_token

# def make_archive(source, name):
#         base = name
#         name = base.split('.')[0]
#         format = base.split('.')[1]
#         archive_from = os.path.dirname(source)
#         archive_to = os.path.basename(source.strip(os.sep))
#         shutil.make_archive(name, format, archive_from, archive_to)
#         # shutil.move('%s.%s'%(name,format), destination)

def make_archive(source, destination):
    base_name = '.'.join(destination.split('.')[:-1])
    format = destination.split('.')[-1]
    root_dir = os.path.dirname(source)
    base_dir = os.path.basename(source.strip(os.sep))
    shutil.make_archive(base_name, format, root_dir, base_dir)
"""
Utility function to poll for starlink terminal gRPC data
in CSV format  
"""
class TerminalGrpcDataCsv:
    def __init__(self, api_path, logfile):
        self.exec_hook = "%s/dish_grpc_text.py" % api_path
        self.logfile = logfile
        self.p = None

    def run(self, _async=True):
        from common import config as cfg
        python_bin = cfg.PYTHON_BIN

        log.info("[grpc_csv] writing CSV headers: logfile=%s exec=%s",
                 self.logfile, self.exec_hook)
        opts = [python_bin, self.exec_hook, "status", "-t", "1"]
        opts_header = opts[:] + ["-H"]
        opts_poll   = opts[:] + ["-O", self.logfile]

        with open(self.logfile, "w") as f:
            log.debug("[grpc_csv] header cmd=%s", opts_header)
            subprocess.call(opts_header, stdout=f)
        log.info("[grpc_csv] CSV headers written to %s", self.logfile)

        log.info("[grpc_csv] starting gRPC poll process: cmd=%s async=%s", opts_poll, _async)
        self.p = subprocess.Popen(opts_poll)
        log.info("[grpc_csv] gRPC poll process started pid=%s", self.p.pid)

        if not _async:
            self.p.wait()
            log.info("[grpc_csv] gRPC poll process exited returncode=%s", self.p.returncode)

    def stop(self):
        if self.p:
            log.info("[grpc_csv] stopping gRPC poll process pid=%s", self.p.pid)
            self.p.kill()
            log.info("[grpc_csv] gRPC poll process killed pid=%s", self.p.pid)
        else:
            log.warning("[grpc_csv] stop() called but process is None — was run() called?")