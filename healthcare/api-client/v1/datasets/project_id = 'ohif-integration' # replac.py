project_id = 'ohif-integration'  # replace with your GCP project ID
location = 'us-central1'  # replace with the dataset's location
dataset_id = 'source'  # replace with the source dataset's ID
destination_dataset_id = 'destination'  # replace with the destination dataset's ID
dicom_store_id = 'source-store'
content_uri = sbucket + "/*.dcm"  # replace with a Cloud Storage bucket and DCM files


def import_dicom_instance(project_id, location, dataset_id, dicom_store_id, content_uri):
    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    api_version = "v1"
    service_name = "healthcare"
    # Returns an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, dataset_id
    )
    dicom_store_name = "{}/dicomStores/{}".format(dicom_store_parent, dicom_store_id)

    body = {"gcsSource": {"uri": "gs://{}".format(content_uri)}}

    # Escape "import()" method keyword because "import"
    # is a reserved keyword in Python
    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .import_(name=dicom_store_name, body=body)
    )

    response = request.execute()
    print("Imported DICOM instance: {}".format(content_uri))

    return response

if __name__ == "__main__":
    deidentify_dataset(0,0,0,0)