from distutils import extension
import logging

import azure.functions as func

from azure.storage.filedatalake import DataLakeServiceClient, DataLakeLeaseClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

import pandas as pd
import os
import sys

from sigpro import get_primitives, discovery
from cms_ml.parsers.cms_texts import parse_med_txt, parse_cms_directory
from cms_ml.pipe_constructor import band_gen, harm_gen
from sigpro.core import SigPro

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Collect the name of the .MED file to process (string: T001_H_20201001_MEDIDAS.MED)
    file = req.params.get('file')
    if not file:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file = req_body.get('file')

    windfarm = req.params.get('windfarm')
    if not windfarm:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            windfarm = req_body.get('windfarm')
    
    path = req.params.get('path')
    if not path:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            path = req_body.get('path')

    agg = req.params.get('agg')
    if not agg:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            agg = req_body.get('agg')


    #discovery.add_primitives_path(os.path.join(sys.prefix, 'Lib/site-packages/cms_ml-0.1.7.dev1-py3.8.egg/cms_ml/primitives/cms_ml/'))
    discovery.add_primitives_path(os.path.join(os.getcwd(), '.python_packages/lib/site-packages/cms_ml/primitives/cms_ml/'))

    #connection_string = "DefaultEndpointsProtocol=https;AccountName=adlsmp;AccountKey=WNk38TUO/zv4natpUzAqoUfwEez1/a8zLc5r068VZWCCSqlKhQojpVWLtQeC/XT/RekMBMhxEOE1+ASt4L8KAw==;EndpointSuffix=core.windows.net"
    storage_account_key = "WNk38TUO/zv4natpUzAqoUfwEez1/a8zLc5r068VZWCCSqlKhQojpVWLtQeC/XT/RekMBMhxEOE1+ASt4L8KAw=="

    # Get the env variable (Function App > Your_Function_App > Configuration > Under Application Settings)
    #KVUri = os.environ["KeyVaultUri"]
    KVUri = "https://kv-smp.vault.azure.net/"

    # Access Key Vault Secrets and specify what secret to access. In this case it's the storage account key
    # for later connection.
    #credential = DefaultAzureCredential()
    #client = SecretClient(vault_url=KVUri, credential=credential)
    #storage_account_key = client.get_secret("storageAccountKey2").value

    storage_account_name = "adlsmp"
    contenedor = "fssmp"
    source_dir = path.rsplit("/", 1)[0]
    sink_dir = "processed_data"
    sink_file = windfarm + "_" + file.split(".")[0] + ".csv"

    # Generate the class DataLakeServiceClient for the ADL Gen2 defined.
    service_client_sink = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=storage_account_key)

    # Generate the instances where files are stored in the ADL.
    # Define the ADL container.
    file_system_client = service_client_sink.get_file_system_client(file_system=contenedor)
    # Define the source and sink directories.
    directory_client_source = file_system_client.get_directory_client(source_dir)

    directory_client_sink = file_system_client.get_directory_client(sink_dir)
    # Define the source and sink files (.MED y .csv, respectively).
    file_client_source = directory_client_source.get_file_client(file)
    file_client_sink = directory_client_sink.create_file(sink_file)

    # Request a new lease for the sink file. This serves that it cannot be
    # modified/deleted by another process (for example, another call from 
    # Azure Functions).
    sink_lease_client = DataLakeLeaseClient(file_client_sink)
    sink_lease_client.acquire()


    # Define where to store the temporary files. In this case, it'll
    # be in "/tmp/", because it's a Linux machine.
    #local_path = os.path.join(os.getcwd(), 'tmp/')
    local_raw_path = "/tmp/"+ file
    local_sink_path = "/tmp/"+ sink_file

    # Create on the local machine the .MED file (empty).
    local_file_prueba = open(local_raw_path, 'wb')
 
    # Download from the ADL the .MED file and store it on the path "/tmp/"
    # for the file just created.
    download = file_client_source.download_file()
    downloaded_bytes = download.readall()
    local_file_prueba.write(downloaded_bytes)

    # TODO controlar las excepciones (aunque no debería pasar nada puesto
    # que se ejecutan los parsers uno a uno).
    # Apply .med parser.
    #data = parse_cms_directory(input_directory = "/tmp", parser = parse_med_txt, extension = 'MED')
    data = parse_med_txt(local_raw_path)

    #data.turbine_id = "T" + data.turbine_id.str.extract('(\d+)')[0].str.zfill(3)
    data['site'] = windfarm

    transformations = [
        {
            "name":"fft",
            "primitive":"sigpro.transformations.amplitude.identity.identity"
        },
        {
            "name":"tracked",
            "primitive":'cms_ml.transformations.amplitude.order_track.shift_frequency',
            "init_params":{
                "nominal_rpm":1680
            }
        }
    ]

    aggregations = []

    for entry in agg:
        aggregations += harm_gen(int(entry['frequency']),int(entry['harmonics']),int(entry['width']), name = entry['name'], primitive = entry['primitive'])

    pipe = SigPro(transformations = transformations, aggregations = aggregations, keep_columns = True, values_column_name = "y_value")

    data_agg, metrics = pipe.process_signal(data = data)
    data_agg.drop(['rms','y_value'], axis=1, inplace=True)

    # Store the generated .csv in the local path "/tmp/"    
    data_agg.to_csv(local_sink_path, encoding="utf-8") 

    # Open on read mode the .csv file created.
    local_file = open(local_sink_path,'r', encoding='utf8')
    file_contents = local_file.read()

    # Break the lease opened beforhand so it can be modified.
    sink_lease_client.break_lease()

    # Upload de data to the ADL.
    file_client_sink.upload_data(data=file_contents, overwrite=True, length=len(file_contents))
    # Commit the data uploaded.
    file_client_sink.flush_data(len(file_contents))

    
    if file:
        return func.HttpResponse(f"The file {file} processed successfully. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a file in the query string or in the request body for a personalized response.",
             status_code=200
        )