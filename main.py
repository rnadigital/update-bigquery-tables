# update-bigquery-tables
# Copyright (C) 2023 RNA Digital Pty Ltd
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
from google.cloud import bigquery

# Get the path to the service account key file from the environment variable
key_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# Create a BigQuery client using the service account key file
client = bigquery.Client.from_service_account_json(key_path)

# Set the project ID to the project you want to query
project_id = 'your-project-id'

# Set the table name to add the columns to for each dataset
TABLE_NAME="example_Table"

# Define the new columns to add to the table
new_columns = [
    bigquery.SchemaField("new_column1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("new_column2", "TIMESTAMP", mode="REQUIRED"),
]

# Loop through each dataset and modify tables with TABLE_NAME in their name
for dataset in client.list_datasets(project=project_id):
    print(f'checking "{dataset.full_dataset_id}" dataset')
    for table in client.list_tables(dataset.dataset_id):
        if TABLE_NAME in table.table_id:
            table_ref = client.dataset(dataset.dataset_id).table(table.table_id)
            table = client.get_table(table_ref)

            # Check if the new columns already exist in the schema
            existing_columns = [field.name for field in table.schema]
            new_columns_to_add = [col for col in new_columns if col.name not in existing_columns]
            if new_columns_to_add:
                table.schema += new_columns_to_add
                client.update_table(table, ["schema"])
                print(f"Table {table.table_id} updated with new columns: {', '.join(col.name for col in new_columns_to_add)}")
            else:
                print(f"Table {table.table_id} already has the new columns.")
