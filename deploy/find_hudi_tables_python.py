import boto3
import sys

# Initialize the S3 client
s3 = boto3.client('s3')

def find_hudi_tables(bucket_name, data_source=None):
    hudi_tables = []

    # Define the prefix based on the data_source provided
    prefix = f"{data_source}/" if data_source else ""

    # List all objects with the given prefix
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
            # If data_source is not specified, get all data_source prefixes
            data_sources = [prefix['Prefix'] for prefix in page.get('CommonPrefixes', [])] if not data_source else [prefix]

            # Loop through each data_source and find potential Hudi tables
            for ds in data_sources:
                # List all tables under each data_source
                table_page = paginator.paginate(Bucket=bucket_name, Prefix=ds, Delimiter='/')
                for table in table_page.search('CommonPrefixes'):
                    table_prefix = table['Prefix']

                    # Check if the ".hoodie" folder exists within the table
                    hoodie_prefix = f"{table_prefix}.hoodie/"
                    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=hoodie_prefix, MaxKeys=1)
                    if 'Contents' in response:
                        # Add the table path to the list if .hoodie folder exists
                        hudi_tables.append(f"s3://{bucket_name}/{table_prefix.rstrip('/')}")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    return hudi_tables


# Example usage
bucket_name = "oh-project-ajax-observed"
data_source = "operations"  # Leave this empty if not specifying a data source
hudi_tables = find_hudi_tables(bucket_name, data_source)

# Output the Hudi table paths
print("Found Hudi tables:")
for table_path in hudi_tables:
    print(table_path)

