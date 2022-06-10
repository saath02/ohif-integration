sbucket = 'dicomsource2'
dbucket = sbucket + "-deid"

project_id = 'ohif-integration'  # replace with your GCP project ID
location = 'us-central1'  # replace with the dataset's location
dataset_id = 'source'  # replace with the source dataset's ID
destination_dataset_id = 'destination'  # replace with the destination dataset's ID
dicom_store_id = 'store'
content_uri = sbucket + str('/**.dcm')  # replace with a Cloud Storage bucket and DCM files

from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import storage
import time


def create_bucket_class_location(dbucket):

    storage_client = storage.Client()

    bucket = storage_client.bucket(dbucket)
    bucket.storage_class = "STANDARD" ##Change to standard
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket


def create_dataset(project_id, location, dataset_id):

    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    api_version = "v1"
    service_name = "healthcare"
    # Instantiates an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)   
    dataset_parent = "projects/{}/locations/{}".format(project_id, location)

    request = (
        client.projects()
        .locations()
        .datasets()
        .create(parent=dataset_parent, body={}, datasetId=dataset_id)
    )

    response = request.execute()
    print("Created dataset: {}".format(dataset_id))
    return response

def create_dicom_store(project_id, location, dataset_id, dicom_store_id):

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

    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .create(parent=dicom_store_parent, body={}, dicomStoreId=dicom_store_id)
    )

    response = request.execute()
    print("Created DICOM store: {}".format(dicom_store_id))
    return response

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
            "dicom": {
                "skipIdRedaction":True,
            },
            "image": {
                "textRedactionMode": "REDACT_ALL_TEXT", ##Can change to only sensitive text
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

def export_dicom_instance(project_id, location, dataset_id, dicom_store_id, uri_prefix):

    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    api_version = "v1"
    service_name = "healthcare"
    # Returns an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)
    uri_prefix = dbucket
    dataset_id = 'destination'
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, dataset_id
    )
    dicom_store_name = "{}/dicomStores/{}".format(dicom_store_parent, dicom_store_id)

    body = {"gcsDestination": {"uriPrefix": "gs://{}".format(uri_prefix)}}

    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .export(name=dicom_store_name, body=body)
    )

    response = request.execute()
    print("Exported DICOM instances to bucket: gs://{}".format(uri_prefix))

    return response

def delete_dataset(project_id, location, dataset_id):

    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    api_version = "v1"
    service_name = "healthcare"
    # Returns an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)
    dataset_name = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, dataset_id
    )

    request = client.projects().locations().datasets().delete(name=dataset_name)

    response = request.execute()
    print("Deleted dataset: {}".format(dataset_id))
    return response


if __name__ == "__main__":
    start = time.time()

    ##create_bucket_class_location(dbucket)
    ##import_dicom_instance(project_id, location, dataset_id, dicom_store_id, content_uri)
    ##deidentify_dataset(project_id, location, dataset_id, destination_dataset_id)
    ##export_dicom_instance(project_id, location, destination_dataset_id, dicom_store_id, 0)
    ##delete_dataset(project_id, location, dataset_id)
    ##delete_dataset(project_id, location, destination_dataset_id)
    ##create_dataset(project_id, location, dataset_id)
    ##create_dicom_store(project_id, location, dataset_id, dicom_store_id) ##Create dataset at end to reduce startup time

    end = time.time()
    print(end - start)