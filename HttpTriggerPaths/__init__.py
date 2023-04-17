import logging

import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


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

    service_client_sink = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=storage_account_key)

    # Generate the instances where files are stored in the ADL.
    # Define the ADL container.
    file_system_client = service_client_sink.get_file_system_client(file_system=contenedor)
    directory_client_sink = file_system_client.get_directory_client("json")    
    file_client_sink = directory_client_sink.create_file("path.json")

    jsonInc = list()

    path_list = file_system_client.get_paths()
    for path in path_list:
        if path.name.startswith("raw_data/") & path.name.endswith(".MED"):
            file = path.name.split("/")[-1]
            windfarm = path.name.split("/")[-2]
            fullPath = path.name
            jsonFor = [{"file":file,"windfarm":windfarm,"path":fullPath}]
            jsonInc += jsonFor

    jsonStr = json.dumps(jsonInc)

    # Upload de data to the ADL.
    file_client_sink.upload_data(data=jsonStr, overwrite=True, length=len(jsonStr))
    # Commit the data uploaded.
    file_client_sink.flush_data(len(jsonStr))

    return json.dumps(jsonInc)

