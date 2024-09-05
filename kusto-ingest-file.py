from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    BlobDescriptor,
    FileDescriptor,
    IngestionProperties,
    IngestionStatus,
    KustoStreamingIngestClient,
    ManagedStreamingIngestClient,
    QueuedIngestClient,
    StreamDescriptor,
)
import config


cluster = "https://ingest-[clustername].brazilsouth.kusto.windows.net" 
client_id = config.client_id
client_secret = config.client_secret
authority_id = config.authority_id


# --------------------
# Solution using Service Principal
# App Id requires ADMINS access in the target Database, or the Forbidden (403-Forbidden) will be raised
# --------------------

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    cluster, client_id, client_secret, authority_id
)
client = QueuedIngestClient(kcsb)


def workload(destinationDb, destinationTable, fileToIngest):
    try:

        # Ingestion Job Properties
        #
        ingestion_props = IngestionProperties(
            database=destinationDb,
            table=destinationTable,
            data_format=DataFormat.CSV,
            ingestion_mapping_reference="table_mapping",
            additional_properties={'ignoreFirstRecord': 'true'}
        )

        # Ingestion itself
        #
        result = client.ingest_from_file(fileToIngest, ingestion_properties=ingestion_props) 

        # Inspect the result for useful information, such as source_id and blob_url
        #
        print(repr(result))

    except KustoServiceError as e:
        print(f"A Kusto error occurred: {e}")
    except Exception as x:
        print(f"An error occurred: {x}")

def main():
    db = "databaseName"
    tb = "tableName"
    workload(destinationDb=db, destinationTable=tb, fileToIngest="dataFilePD5.csv")

if __name__ == "__main__":
    main()
