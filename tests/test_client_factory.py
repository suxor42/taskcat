import unittest

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound

import mock
from taskcat._client_factory import Boto3Cache
from taskcat.exceptions import TaskCatException


class TestBoto3Cache(unittest.TestCase):

    @mock.patch("taskcat._client_factory.Boto3Cache.session", autospec=True)
    def test_resource(self, mock_session):
        Boto3Cache().resource("s3")
        self.assertEqual(mock_session.called, True)

    @mock.patch("taskcat._client_factory.boto3", autospec=True)
    @mock.patch("taskcat._client_factory.Boto3Cache.session")
    def test__get_account_info(
        self, mock_session, mock_boto3
    ):
        session = boto3.Session()
        session.client = mock.Mock()
        sts = mock.Mock()
        sts.get_caller_identity.return_value = {"Account": "123412341234"}
        session.client.return_value = sts
        mock_session.return_value = session
        mock_boto3.session.Session = mock.Mock()
        mock_boto3.session.Session.return_value = session
        cache = Boto3Cache()

        partition = cache.partition("us-gov-east-1")
        self.assertEqual(partition, "aws-us-gov")

        partition = cache.partition("cn-north-1")
        self.assertEqual(partition, "aws-cn")

        partition = cache.partition("us-east-1")
        self.assertEqual(partition, "aws")

        sts.get_caller_identity.side_effect = ClientError(
            error_response={"Error": {"Code": "test"}}, operation_name="test"
        )
        with self.assertRaises(ClientError):
            cache._get_account_info("default")

        sts.get_caller_identity.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied"}}, operation_name="test"
        )
        with self.assertRaises(TaskCatException):
            cache._get_account_info("default")

        sts.get_caller_identity.side_effect = NoCredentialsError()
        with self.assertRaises(TaskCatException):
            cache._get_account_info("default")

        sts.get_caller_identity.side_effect = ProfileNotFound(
            profile="non-existent_profile"
        )
        with self.assertRaises(TaskCatException):
            cache._get_account_info("default")

    @mock.patch("taskcat._client_factory.boto3", autospec=True)
    @mock.patch("taskcat._client_factory.Boto3Cache.session")
    def test__get_partition(self, mock_session, mock_boto3):
        session = boto3.Session()
        session.client = mock.Mock()
        sts = mock.Mock()
        sts.get_caller_identity.return_value = {"Account": "123412341234"}
        session.client.return_value = sts
        mock_session.return_value = session
        mock_boto3.session.Session = mock.Mock()
        mock_boto3.session.Session.return_value = session
        cache = Boto3Cache()

        invalid_token_exception = ClientError(
            error_response={"Error": {"Code": "InvalidClientTokenId"}},
            operation_name="test",
        )

        sts.get_caller_identity.side_effect = [True]
        result = cache.partition("us-east-1")
        self.assertEqual(result, "aws")

        sts.get_caller_identity.side_effect = [invalid_token_exception, True]
        result = cache.partition("cn-north-1")
        self.assertEqual(result, "aws-cn")

        sts.get_caller_identity.side_effect = [
            invalid_token_exception,
            invalid_token_exception,
            True,
        ]
        result = cache.partition("us-gov-west-1")
        self.assertEqual(result, "aws-us-gov")
