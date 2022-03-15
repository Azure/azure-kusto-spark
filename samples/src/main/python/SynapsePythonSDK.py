# This sample shows how to use the Kusto Python SDK with Synapse authentication.
# One should use the SDK to run admin commands while using the connector to query
# https://docs.microsoft.com/azure/data-explorer/kusto/api/python/kusto-python-client-library
# To upload SDK package to synapse workspace, create a wheel file and follow this:
# https://docs.microsoft.com/azure/synapse-analytics/spark/apache-spark-manage-python-packages#install-wheel-files
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

token_provider = lambda: TokenLibrary().getAccessToken("{\"audience\": \"AzureDataExplorer\", \"name\": \"\"}").getToken()
db = "db"
query = "table"

kcsb = KustoConnectionStringBuilder.with_token_provider("https://{cluster_name}.kusto.windows.net", token_provider)
kusto_client = KustoClient(kcsb)
response = kusto_client.execute(db, query)

for row in response.primary_results[0]:
        print(row[0])