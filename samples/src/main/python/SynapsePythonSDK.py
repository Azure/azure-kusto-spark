# This sample shows how to use the Kusto Python SDK with Synapse Spark utilities for authentication, as described here:
# https://docs.microsoft.com/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python#credentials-utilities.
# One should use the SDK to run admin commands while using the connector to query (as in SynapseSample.py)
# https://docs.microsoft.com/azure/data-explorer/kusto/api/python/kusto-python-client-library
# To upload SDK package to synapse workspace, create a wheel file (if only the Python SDK is needed - clone this repo:
# https://github.com/Azure/azure-kusto-python) and then follow this:
# https://docs.microsoft.com/azure/synapse-analytics/spark/apache-spark-manage-python-packages#install-wheel-files
# In this sample, the principal should have at least database monitor privileges.
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

# A token provider that uses the linked service authentication method provided in its creation.
token_provider = lambda: mssparkutils.credentials.getConnectionStringOrCreds("{linked_service_name}")

# A token provider that uses the user's credentials in interactive mode via MSI.
user_token_provider = lambda: TokenLibrary().getAccessToken("{\"audience\": \"AzureDataExplorer\", \"name\": \"\"}")
.getToken()
db = "db"
cmd = ".show table d2 schema as json"

kcsb = KustoConnectionStringBuilder.with_token_provider("https://{cluster_name}.kusto.windows.net", token_provider)
kusto_client = KustoClient(kcsb)
response = kusto_client.execute(db, cmd)

for row in response.primary_results[0]:
        print("table:", row[0], "schema:", row[1], "database:", row[2])