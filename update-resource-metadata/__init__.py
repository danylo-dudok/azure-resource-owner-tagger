import datetime
import logging
import json

import azure.functions as func

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import HttpResponseError


def main(event: func.EventGridEvent) -> None:
    IGNORED_RESOURCES = [
        "Microsoft.Resources/deployments",
        "Microsoft.Resources/tags",
    ]
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    try:
        id, email, resource = extract_metadata(event.get_json())
        if any([ignored_resource in resource for ignored_resource in IGNORED_RESOURCES]):
            return

        assign_owner(id, email, resource)
    except Exception as err:
        logging.error(f"Error happened during runtime. With resource: {resource}", exc_info=err)
    finally:
        logging.info('Python event trigger function ran at %s', utc_timestamp)


def extract_metadata(json: dict) -> tuple[str, str, str]:
    EMAIL_CLAIM_KEY = "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name"
    ID_CLAIM_KEY = "http://schemas.microsoft.com/identity/claims/objectidentifier"
    CLAIMS_KEY = "claims"
    RESOURCE_KEY = "resourceUri"

    id = None
    email = None
    resource = None

    if CLAIMS_KEY in json:
        authorization_claims = json[CLAIMS_KEY]

        if EMAIL_CLAIM_KEY in authorization_claims:
            email = authorization_claims[EMAIL_CLAIM_KEY]

        if ID_CLAIM_KEY in authorization_claims:
            id = authorization_claims[ID_CLAIM_KEY]

    if RESOURCE_KEY in json:
        resource = extract_resource_path(json[RESOURCE_KEY])

    return id, email, resource


def assign_owner(user_id: str, email: str, resource: str) -> None:
    USER_ID_TAG_NAME = "owner_id"
    EMAIL_TAG_NAME = "owner_email"
    tags = {
        USER_ID_TAG_NAME: user_id,
        EMAIL_TAG_NAME: email if email else "ManagedIdentity",
    }

    subscription_id = extract_subscription_id(resource)
    with ResourceManagementClient(DefaultAzureCredential(), subscription_id) as resource_client:
        path = resource.split("/")
        resource_group_name = path[4]
        resource_type = f"{path[6]}/{path[7]}"
        resource_name = path[8]

        azure_resource = call_until_valid_api(resource_client, resource_group_name, resource_type, resource_name)
        if azure_resource and not azure_resource.tags or (
                USER_ID_TAG_NAME not in azure_resource.tags and EMAIL_TAG_NAME not in azure_resource.tags):
            resource_client.tags.create_or_update_at_scope(resource, {"properties": {"tags": tags}})


def call_until_valid_api(
        resource_client: ResourceManagementClient,
        resource_group_name: str,
        resource_type: str,
        resource_name: str,
        api_version: str = None) -> any:
    API_VERSION = "2021-04-01"
    try:
        return resource_client.resources.get(
            resource_group_name=resource_group_name,
            resource_provider_namespace="",
            parent_resource_path="",
            resource_type=resource_type,
            resource_name=resource_name,
            api_version=api_version if api_version else API_VERSION
        )
    except HttpResponseError as error:
        if api_version:
            raise error

        if error.status_code == 400:
            text = error.response.text()
            error_json = json.loads(text)
            if error_json["error"]["code"] == "NoRegisteredProviderFound":
                api_version = extract_version_from_error_message(error_json["error"]["message"])
                return call_until_valid_api(
                    resource_client,
                    resource_group_name,
                    resource_type,
                    resource_name,
                    api_version
                )


def extract_version_from_error_message(message: str) -> str:
    versions = message.split("The supported api-versions are '")[1]
    return versions.split(",")[0]


def extract_resource_path(path: str) -> str:
    """Cuts resource path to resource name"""
    return "/".join(path.split("/")[:9])


def extract_subscription_id(path: str) -> str:
    """Get only subscription id from full resource path"""
    return path.split("/")[2]
