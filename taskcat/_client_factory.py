import logging
from typing import Dict

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound

from taskcat.exceptions import TaskCatException

LOG = logging.getLogger(__name__)

REGIONAL_ENDPOINT_SERVICES = ["sts"]
STS_SERVICE_NAME = 'sts'


class Boto3Cache:
    def __init__(self, _boto3=boto3):
        self._boto3 = _boto3
        self._account_info: Dict[str, str] = {}
        self._region_partition_map = None
        self._session_cache: Dict[str, boto3.Session] = {}

    def session(self, profile: str = "default", region: str = None) -> boto3.Session:
        return self._session_cache.setdefault(profile, self._boto3.Session(profile_name=profile, region_name=region))

    def client(
        self, service: str, profile: str = "default", region: str = None
    ) -> boto3.client:
        return self.session(profile).client(service_name=service, region_name=region)

    def resource(
        self, service: str, profile: str = "default", region: str = None
    ) -> boto3.resource:
        return self.session(profile).resource(service_name=service, region_name=region)

    def partition(self, region: str) -> str:
        if self._region_partition_map is None:
            partitions = self._boto3.Session().get_available_partitions()
            partition_region_map = {}
            for partition in partitions:
                partition_region_map[partition] = self._boto3.Session().get_available_regions(
                    service_name=STS_SERVICE_NAME, partition_name=partition)
            self._region_partition_map = {}
            for partition, regions in partition_region_map.items():
                for single_region in regions:
                    self._region_partition_map[single_region] = partition
        return self._region_partition_map.get(region, "aws")

    def account_id(self, profile: str = "default") -> str:
        return self._account_info.setdefault(profile, self._get_account_info(profile))

    def _get_account_info(self, profile) -> str:
        session = self.session(profile)
        region = session.region_name
        sts_client = session.client("sts", region_name=region)
        try:
            account_id = sts_client.get_caller_identity()["Account"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDenied":
                raise TaskCatException(
                    f"Not able to fetch account number using profile "
                    f"{profile}. {str(e)}"
                )
            raise
        except NoCredentialsError as e:
            raise TaskCatException(
                f"Not able to fetch account number using profile "
                f"{profile}. {str(e)}"
            )
        except ProfileNotFound as e:
            raise TaskCatException(
                f"Not able to fetch account number using profile "
                f"{profile}. {str(e)}"
            )
        return account_id

    def get_default_region(self, profile_name="default") -> str:
        try:
            session = self._boto3.session.Session(profile_name=profile_name)
            region = session.region_name
        except ProfileNotFound:
            if profile_name != "default":
                raise
            region = self._boto3.session.Session().region_name
        if not region:
            region = "us-east-1"
            LOG.warning(
                f"Region not set in credential chain, defaulting to {region}"
            )
        return region
