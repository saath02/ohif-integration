project_id = 'ohif-integration'  # replace with your GCP project ID
location = 'us-central1'  # replace with the dataset's location
dataset_id = 'source'  # replace with the source dataset's ID
destination_dataset_id = 'destination'  # replace with the destination dataset's ID
dicom_store_id = 'source-store'

def deidentify_dataset(project_id, location, dataset_id, destination_dataset_id):
    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    api_version = "v1"
    service_name = "healthcare"
    # Returns an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)
    source_dataset = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, dataset_id
    )
    destination_dataset = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, destination_dataset_id
    )

    body = {
        "destinationDataset": destination_dataset,
        "config": {
            "dicom": {},
            "image": {
                "textRedactionMode": "REDACT_ALL_TEXT",
            }
        },
    }

    request = (
        client.projects()
        .locations()
        .datasets()
        .deidentify(sourceDataset=source_dataset, body=body)
    )

    response = request.execute()
    print(
        "Data in dataset {} de-identified."
        "De-identified data written to {}".format(dataset_id, destination_dataset_id)
    )
    return response

if __name__ == "__main__":
    deidentify_dataset(0,0,0,0)