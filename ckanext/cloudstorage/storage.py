#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
import os
import six
from six.moves.urllib.parse import urljoin
from ast import literal_eval
from datetime import datetime, timedelta
import traceback
from ckan import model
from ckan.lib import munge
import ckan.plugins as p
import hashlib
import binascii

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver
import libcloud.common.types as types

if p.toolkit.check_ckan_version("2.9"):
    from werkzeug.datastructures import FileStorage as UploadedFileType

    config = p.toolkit.config
else:
    from pylons import config

    UploadedFileType = cgi.FieldStorage

from werkzeug.datastructures import FileStorage as FlaskFileStorage

ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)
AWS_UPLOAD_PART_SIZE = 5 * 1024 * 1024

import logging

logger = logging.getLogger(__name__)


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


def _md5sum(source_path):
    block_count = 0
    block = True
    md5string = b""
    with open(source_path, "rb") as f:
        while block:
            block = f.read(AWS_UPLOAD_PART_SIZE)
            if block:
                block_count += 1
                hash_obj = hashlib.md5()
                hash_obj.update(block)
                md5string = md5string + binascii.unhexlify(hash_obj.hexdigest())
            else:
                break
    hash_obj = hashlib.md5()
    hash_obj.update(md5string)
    return hash_obj.hexdigest() + "-" + str(block_count)


class CloudStorage(object):
    def __init__(self):
        self.driver = get_driver(getattr(Provider, self.driver_name))(
            **self.driver_options
        )
        self._container = None

    def path_from_filename(self, rid, filename):
        raise NotImplementedError

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return literal_eval(config["ckanext.cloudstorage.driver_options"])

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config["ckanext.cloudstorage.driver"]

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config["ckanext.cloudstorage.container_name"]

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.use_secure_urls", False)
        )

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(config.get("ckanext.cloudstorage.leave_files", False))

    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if self.driver_name == "AZURE_BLOBS":
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage

                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if "S3" in self.driver_name:
            if "host" not in self.driver_options:
                # newer libcloud versions(must-use for python3)
                # requires host for secure URLs
                return False
            try:
                # Yes? Is the boto package available?
                import boto

                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                logger.info(
                    "Can not import 'boto', urls cannot be secured for download"
                )
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.guess_mimetype", False)
        )


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop("upload", None)
        self._clear = resource.pop("clear_upload", None)
        multipart_name = resource.pop("multipart_name", None)

        # Check to see if a file has been provided
        if (
            isinstance(upload_field_storage, (ALLOWED_UPLOAD_TYPES))
            and upload_field_storage.filename
        ):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = _get_underlying_file(upload_field_storage)
            resource["url"] = self.filename
            resource["url_type"] = "upload"
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource["url"] = munge.munge_filename(multipart_name)
            resource["url_type"] = "upload"
        elif self._clear and resource.get("id"):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(model.Resource).get(resource["id"])

            self.old_filename = old_resource.url
            resource["url_type"] = ""

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join("resources", rid, munge.munge_filename(filename))

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        if self.filename:
            if self.can_use_advanced_azure:
                from azure.storage import blob as azure_blob
                from azure.storage.blob.models import ContentSettings

                blob_service = azure_blob.BlockBlobService(
                    self.driver_options["key"], self.driver_options["secret"]
                )
                content_settings = None
                if self.guess_mimetype:
                    content_type, _ = mimetypes.guess_type(self.filename)
                    if content_type:
                        content_settings = ContentSettings(content_type=content_type)
                return blob_service.create_blob_from_stream(
                    container_name=self.container_name,
                    blob_name=self.path_from_filename(id, self.filename),
                    stream=self.file_upload,
                    content_settings=content_settings,
                )
            else:
                try:
                    file_upload = self.file_upload
                    # in Python3 libcloud iterates over uploaded file,
                    # while it's wrappend into non-iterator. So, pick real
                    # file-object and give it to cloudstorage
                    # if six.PY3:
                    #    file_upload = file_upload._file

                    # self.container.upload_object_via_stream(
                    #     file_upload,
                    #     object_name=self.path_from_filename(
                    #         id,
                    #         self.filename
                    #     )
                    # )

                    # check if already uploaded
                    object_name = self.path_from_filename(id, self.filename)
                    try:
                        cloud_object = self.container.get_object(
                            object_name=object_name
                        )
                        print(
                            "\t Object found, checking size {0}: {1}".format(
                                object_name, cloud_object.size
                            )
                        )
                        file_size = os.path.getsize(file_upload.name)
                        print(
                            "\t - File size {0}: {1}".format(
                                file_upload.name, file_size
                            )
                        )
                        if file_size == int(cloud_object.size):
                            print(
                                "\t Size fits, checking hash {0}: {1}".format(
                                    object_name, cloud_object.hash
                                )
                            )
                            hash_file = hashlib.md5(
                                open(file_upload.name, "rb").read()
                            ).hexdigest()
                            print(
                                "\t - File hash {0}: {1}".format(
                                    file_upload.name, hash_file
                                )
                            )
                            # basic hash
                            if hash_file == cloud_object.hash:
                                print(
                                    "\t => File found, matching hash, skipping upload"
                                )
                                return
                            # multipart hash
                            multi_hash_file = _md5sum(file_upload.name)
                            print(
                                "\t - File multi hash {0}: {1}".format(
                                    file_upload.name, multi_hash_file
                                )
                            )
                            if multi_hash_file == cloud_object.hash:
                                print(
                                    "\t => File found, matching hash, skipping upload"
                                )
                                return
                        print("\t Resource found in the cloud but outdated, uploading")
                    except ObjectDoesNotExistError:
                        print("\t Resource not found in the cloud, uploading")

                    # FIX: replaced call with a simpler version
                    with open(file_upload.name, "rb") as iterator:
                        self.container.upload_object_via_stream(
                            iterator=iterator, object_name=object_name
                        )
                    print(
                        "\t => UPLOADED {0}: {1}".format(file_upload.name, object_name)
                    )
                except ValueError as v:
                    print(traceback.format_exc())
                    raise v
                except types.InvalidCredsError as err:
                    print(traceback.format_exc())
                    raise err

        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            try:
                self.container.delete_object(
                    self.container.get_object(
                        self.path_from_filename(id, self.old_filename)
                    )
                )
            except ObjectDoesNotExistError:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def get_url_from_filename(self, rid, filename, content_type=None):
        """
        Retrieve a publically accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param content_type: Optionally a Content-Type header.

        :returns: Externally accessible URL or None.
        """
        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)

        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
            from azure.storage import blob as azure_blob

            blob_service = azure_blob.BlockBlobService(
                self.driver_options["key"], self.driver_options["secret"]
            )

            return blob_service.make_blob_url(
                container_name=self.container_name,
                blob_name=path,
                sas_token=blob_service.generate_blob_shared_access_signature(
                    container_name=self.container_name,
                    blob_name=path,
                    expiry=datetime.utcnow() + timedelta(hours=1),
                    permission=azure_blob.BlobPermissions.READ,
                ),
            )
        elif self.can_use_advanced_aws and self.use_secure_urls:
            from boto.s3.connection import S3Connection

            os.environ["S3_USE_SIGV4"] = "True"
            s3_connection = S3Connection(
                self.driver_options["key"],
                self.driver_options["secret"],
                host=self.driver_options["host"],
            )

            if "region_name" in self.driver_options.keys():
                s3_connection.auth_region_name = self.driver_options["region_name"]

            generate_url_params = {
                "expires_in": 60 * 60,
                "method": "GET",
                "bucket": self.container_name,
                "key": path,
            }
            if content_type:
                generate_url_params["headers"] = {"Content-Type": content_type}
            return s3_connection.generate_url_sigv4(**generate_url_params)

        # Find the object for the given key.
        try:
            obj = self.container.get_object(path)
        except ObjectDoesNotExistError:
            return
        if obj is None:
            return

        # Not supported by all providers!
        try:
            return self.driver.get_object_cdn_url(obj)
        except NotImplementedError:
            if "S3" in self.driver_name:
                return urljoin(
                    "https://" + self.driver.connection.host,
                    "{container}/{path}".format(
                        container=self.container_name, path=six.ensure_str(path)
                    ),
                )
            # This extra 'url' property isn't documented anywhere, sadly.
            # See azure_blobs.py:_xml_to_object for more.
            elif "url" in obj.extra:
                return obj.extra["url"]
            raise

    def get_s3_signed_url_download(self, rid, filename, content_type=None):
        """
        Retrieve a signed URL to download a multipart part from the frontend.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param content_type: Optionally a Content-Type header.

        :returns: Signed URL or None.
        """

        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)

        # parameters
        bucket = self.container_name
        key = path
        expiresIn = 3600

        if self.can_use_advanced_aws and self.use_secure_urls:
            from boto.s3.connection import S3Connection
            import boto

            os.environ["S3_USE_SIGV4"] = "True"

            s3_connection = S3Connection(
                self.driver_options["key"],
                self.driver_options["secret"],
                host=self.driver_options["host"],
            )

            s3_connection.auth_region_name = self.driver_options["region_name"]

            signed_url = s3_connection.generate_url_sigv4(
                expires_in=expiresIn, method="GET", bucket=bucket, key=key
            )

            return signed_url

        return None

    def get_s3_signed_url_multipart(
        self, rid, filename, upload_id, part_number, content_type=None
    ):
        """
        Retrieve a signed URL to upload a multipart part from the frontend.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param upload_id: The multipart upload ID.
        :param part_number: The part number from an array of multiparts.
        :param content_type: Optionally a Content-Type header.

        :returns: Signed URL or None.
        """

        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)

        # parameters
        bucket = self.container_name
        key = path
        conditions = []
        fields = []
        expiresIn = 3600

        if self.can_use_advanced_aws and self.use_secure_urls:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                aws_access_key_id=self.driver_options["key"],
                aws_secret_access_key=self.driver_options["secret"],
                endpoint_url="https://" + self.driver_options["host"],
                region_name=self.driver_options["region_name"],
            )

            signed_url = client.generate_presigned_url(
                ClientMethod="upload_part",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                    "UploadId": upload_id,
                    "PartNumber": part_number,
                },
                ExpiresIn=expiresIn,
            )

            return signed_url

        return "notAvailable"

    def get_s3_multipart_parts(self, upload_id, key=None, rid=None, filename=None):
        """
        Retrieve an array of S3 Part objects for a given multipart upload.
        Requires either 'key', or 'rid' and 'filename'

        :param upload_id: The multipart upload ID.
        :type upload_id: str
        :param key: The multipart upload key path.
        :type key: str, optional
        :param rid: The resource ID.
        :type rid: int, optional
        :param filename: The resource filename.
        :type filename: str, optional
        

        :returns: Array of S3 Part objects or None.
        """

        if key is None:
            # Find the key the file *should* be stored at.
            key = self.path_from_filename(rid, filename)

        bucket = self.container_name

        if self.can_use_advanced_aws and self.use_secure_urls:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                aws_access_key_id=self.driver_options["key"],
                aws_secret_access_key=self.driver_options["secret"],
                endpoint_url="https://" + self.driver_options["host"],
                region_name=self.driver_options["region_name"],
            )

            parts_info = client.list_parts(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )

            return parts_info["Parts"]

        return "notAvailable"

    @property
    def package(self):
        return model.Package.get(self.resource["package_id"])
