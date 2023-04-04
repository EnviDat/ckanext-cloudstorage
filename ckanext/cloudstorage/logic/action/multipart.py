#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import datetime

import libcloud.security

from sqlalchemy.orm.exc import NoResultFound
import ckan.model as model
import ckan.logic as logic
import ckan.lib.helpers as h
import ckan.plugins.toolkit as toolkit

import requests

from ckanext.cloudstorage.storage import ResourceCloudStorage
from ckanext.cloudstorage.model import MultipartUpload, MultipartPart
from werkzeug.datastructures import FileStorage as FlaskFileStorage

if toolkit.check_ckan_version("2.9"):
    config = toolkit.config
else:
    from pylons import config

import sys, traceback


libcloud.security.VERIFY_SSL_CERT = True

log = logging.getLogger(__name__)


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


def _get_max_multipart_lifetime():
    value = float(config.get("ckanext.cloudstorage.max_multipart_lifetime", 7))
    return datetime.timedelta(value)


def _get_object_url(uploader, name):
    return "/" + uploader.container_name + "/" + name


def _delete_multipart(upload, uploader):
    log.debug(
        "_delete_multipart url {0}".format(_get_object_url(uploader, upload.name))
    )
    log.debug("_delete_multipart id {0}".format(upload.id))
    resp = uploader.driver.connection.request(
        _get_object_url(uploader, upload.name),
        params={
            "uploadId": upload.id
            #            'partNumber': part_number
        },
        method="DELETE",
    )

    if not resp.success():
        raise toolkit.ValidationError(resp.error)

    upload.delete()
    upload.commit()
    return resp


def _save_part_info(n, etag, upload):
    try:
        part = (
            model.Session.query(MultipartPart)
            .filter(MultipartPart.n == n, MultipartPart.upload == upload)
            .one()
        )
    except NoResultFound:
        part = MultipartPart(n, etag, upload)
    else:
        part.etag = etag
    part.save()
    return part


def check_multipart(context, data_dict):
    """Check whether unfinished multipart upload already exists.

    :param context:
    :param data_dict: dict with required `id`
    :returns: None or dict with `upload` - existing multipart upload info
    :rtype: NoneType or dict

    """
    h.check_access("cloudstorage_check_multipart", data_dict)
    id = toolkit.get_or_bust(data_dict, "id")

    try:
        upload = model.Session.query(MultipartUpload).filter_by(resource_id=id).one()
    except NoResultFound:
        log.error("check_multipart return None")
        return
    upload_dict = upload.as_dict()
    upload_dict["parts"] = (
        model.Session.query(MultipartPart)
        .filter(MultipartPart.upload == upload)
        .count()
    )
    return {"upload": upload_dict}


def initiate_multipart(context, data_dict):
    """Initiate new Multipart Upload.

    :param context:
    :param data_dict: dict with required keys:
        id: resource's id
        name: filename
        size: filesize

    :returns: MultipartUpload info
    :rtype: dict

    """
    log.debug("initiate_multipart")

    h.check_access("cloudstorage_initiate_multipart", data_dict)
    id, name, size = toolkit.get_or_bust(data_dict, ["id", "name", "size"])
    user_obj = model.User.get(context["user"])
    user_id = user_obj.id if user_obj else None

    uploader = ResourceCloudStorage({"multipart_name": name})
    res_name = uploader.path_from_filename(id, name)

    upload_object = MultipartUpload.by_name(res_name)

    log.debug("initiate_multipart upload_object={0}".format(upload_object))

    if upload_object is not None:
        _delete_multipart(upload_object, uploader)
        upload_object = None

    if upload_object is None:
        for old_upload in model.Session.query(MultipartUpload).filter_by(
            resource_id=id
        ):
            log.debug("initiate_multipart delete old_upload={0}".format(old_upload))
            _delete_multipart(old_upload, uploader)

        # Find and remove previous file from this resource
        _rindex = res_name.rfind("/")
        if ~_rindex:
            try:
                name_prefix = res_name[:_rindex]
                old_objects = uploader.driver.iterate_container_objects(
                    uploader.container, name_prefix
                )
                for obj in old_objects:
                    log.info("Removing cloud object: %s" % obj)
                    obj.delete()
            except Exception as e:
                log.exception("[delete from cloud] %s" % e)

        upload_object = MultipartUpload(
            uploader.driver._initiate_multipart(
                container=uploader.container, object_name=res_name
            ),
            id,
            res_name,
            size,
            name,
            user_id,
        )

        upload_object.save()
    return upload_object.as_dict()


def upload_multipart(context, data_dict):
    h.check_access("cloudstorage_upload_multipart", data_dict)
    log.debug(" upload_multipart dict: {0}".format(data_dict))
    upload_id, part_number, part_content = toolkit.get_or_bust(
        data_dict, ["uploadId", "partNumber", "upload"]
    )
    log.debug(" upload_multipart upload id: {0}".format(upload_id))

    uploader = ResourceCloudStorage({})
    upload = model.Session.query(MultipartUpload).get(upload_id)
    data = _get_underlying_file(part_content).read()
    resp = uploader.driver.connection.request(
        _get_object_url(uploader, upload.name),
        params={"uploadId": upload_id, "partNumber": part_number},
        method="PUT",
        headers={"Content-Length": len(data)},
        data=data,
    )
    if resp.status != 200:
        raise toolkit.ValidationError("Upload failed: part %s" % part_number)

    _save_part_info(part_number, resp.headers["etag"], upload)
    return {"partNumber": part_number, "ETag": resp.headers["etag"]}


@toolkit.side_effect_free
def get_presigned_url_multipart(context, data_dict):
    log.debug("get_presigned_url_multipart")

    h.check_access("cloudstorage_get_presigned_url_multipart", data_dict)

    signed_url = None

    try:
        rid, upload_id, part_number, filename = toolkit.get_or_bust(
            data_dict, ["id", "uploadId", "partNumber", "filename"]
        )
        log.debug(
            f"Resource ID: {rid} | Upload ID: {upload_id} "
            f"| Part number: {part_number} | File name: {filename}"
        )

        uploader = ResourceCloudStorage({})

        log.debug(f"Signing URL for upload id: {upload_id}")
        signed_url = uploader.get_s3_signed_url_multipart(
            rid, filename, upload_id, int(part_number)
        )
    except Exception as e:
        log.error("EXCEPTION get_presigned_url_multipart: {0}".format(e))
        traceback.print_exc(file=sys.stderr)

    log.debug(f"Presigned URL: {signed_url}")
    return signed_url


@toolkit.side_effect_free
def get_presigned_url_list_multipart(context, data_dict):
    log.debug("get_presigned_url_list_multipart")

    h.check_access("cloudstorage_get_presigned_url_multipart", data_dict)

    presignedUrls = {}

    try:
        rid, upload_id, part_number_list, filename = toolkit.get_or_bust(
            data_dict, ["id", "uploadId", "partNumbersList", "filename"]
        )
        log.debug(
            f"Resource ID: {rid} | Upload ID: {upload_id} "
            f"| Part number list: {part_number_list} | File name: {filename}"
        )

        uploader = ResourceCloudStorage({})

        for part_number in part_number_list:
            log.debug(f"Signing URL for part: {part_number} upload id: {upload_id}")
            signed_url = uploader.get_s3_signed_url_multipart(
                rid, filename, upload_id, int(part_number)
            )
            presignedUrls[part_number] = signed_url

    except Exception as e:
        log.error("EXCEPTION get_presigned_url_list_multipart: {0}".format(e))
        traceback.print_exc(file=sys.stderr)

    log.debug(f"Presigned URLs: {presignedUrls}")
    return {"presignedUrls": presignedUrls}


@toolkit.side_effect_free
def get_presigned_url_download(context, data_dict):

    """Return the direct cloud download link for a resource.

    :param id: the id of the resource
    :type id: string

    :url: string

    """

    log.debug("get_presigned_url_download")
    signed_url = None

    id = toolkit.get_or_bust(data_dict, "id")

    model = context["model"]
    resource = model.Resource.get(id)
    resource_context = dict(context, resource=resource)

    if not resource:
        raise logic.NotFound

    toolkit.check_access("resource_show", resource_context, data_dict)

    # if resource type is url, return its url
    if resource.url_type != "upload":
        return resource.url

    # request a presigned GET url
    try:
        name = resource.url
        uploader = ResourceCloudStorage({})
        log.debug(f"Signing URL to download resource id: {id}")
        signed_url = uploader.get_s3_signed_url_download(id, name)
    except Exception as e:
        log.error("EXCEPTION: {0}".format(e))
        traceback.print_exc(file=sys.stderr)
        raise e

    if not signed_url:
        raise toolkit.ValidationError(
            "Cannot provide a URL. Cloud storage not compatible."
        )

    log.debug(f"Presigned URL: {signed_url}")
    return signed_url


@toolkit.side_effect_free
def multipart_list_parts(context, data_dict):
    log.debug("multipart_list_parts")

    # h.check_access("cloudstorage_multipart_list_parts", data_dict)

    multipart_parts = {}

    try:
        upload_id = toolkit.get_or_bust(data_dict, "uploadId")

        if (upload_key := data_dict.get("uploadKey")) is not None:
            rid = None
            filename = None
        else:
            rid, filename = toolkit.get_or_bust(data_dict, ["id", "filename"])
            upload_key = None
        uploader = ResourceCloudStorage({})
        log.debug(
            f"Upload ID: {upload_id} | Upload Key: {upload_key} | "
            f"Resource ID: {rid} | File name: {filename}"
        )
        multipart_parts = uploader.get_s3_multipart_parts(
            upload_id, key=upload_key, rid=rid, filename=filename
        )
        # Instead of json encoding datetime, simply remove LastModified
        for part in multipart_parts:
            part.pop("LastModified", None)

    except Exception as e:
        log.error(f"EXCEPTION multipart_list_parts: {e}")
        traceback.print_exc(file=sys.stderr)

    log.debug(f"Multipart parts: {multipart_parts}")
    return multipart_parts


def upload_multipart_presigned(context, data_dict):
    """
    Helper function to generate and upload via pre-signed URL in one endpoint.
    Required for edge cases only.
    """
    log.debug("upload_multipart_presigned")

    h.check_access("cloudstorage_upload_multipart_presigned", data_dict)

    upload_id, part_number, part_content = toolkit.get_or_bust(
        data_dict, ["uploadId", "partNumber", "upload"]
    )
    log.debug(f"Upload ID: {upload_id} | Part Number: {part_number}")

    log.debug("Generating presigned url.")
    presigned_url = get_presigned_url_multipart(context, data_dict)

    upload = model.Session.query(MultipartUpload).get(upload_id)
    data = _get_underlying_file(part_content).read()

    log.debug("Uploading...")
    resp = requests.put(presigned_url, data=data)

    if resp.status_code != 200:
        raise toolkit.ValidationError(
            "Upload failed ({0}: part {1}".format(resp.status_code, part_number)
        )

    _save_part_info(part_number, resp.headers["etag"], upload)

    return {"partNumber": part_number, "ETag": resp.headers["etag"]}


def finish_multipart(context, data_dict):
    """Called after all parts had been uploaded.

    Triggers call to `_commit_multipart` which will convert separate uploaded
    parts into single file

    :param context:
    :param data_dict: dict with required key `uploadId` - id of Multipart Upload that should be finished
    :returns: S3 url and commit confirmation
    :rtype: dict

    """

    log.debug("finish_multipart.")
    h.check_access("cloudstorage_finish_multipart", data_dict)
    upload_id = toolkit.get_or_bust(data_dict, "uploadId")
    log.debug(f"upload_id: {upload_id}")
    try:
        import json

        json_string = toolkit.get_or_bust(data_dict, "partInfo")
        json_string = (
            json_string.replace("'", '"').replace('\\"', "").replace('""', '"')
        )
        part_info = json.loads(json_string)
        log.debug(f"part_info: {part_info}")
    except toolkit.ValidationError as e:
        part_info = False
        log.debug("partInfo not found in data_dict, assuming not multipart")
    save_action = data_dict.get("save_action", False)
    upload = model.Session.query(MultipartUpload).get(upload_id)
    log.debug(f"Multipart upload record from database: {upload}")
    if part_info:
        chunks = [(part["PartNumber"], part["ETag"]) for part in part_info]
    else:
        log.debug("Uploaded from CKAN UI, getting chunk records from DB")
        chunk_db = (
            model.Session.query(MultipartPart)
            .filter_by(upload_id=upload_id)
            .order_by(MultipartPart.n)
        )
        chunks = [(part.n, part.etag) for part in chunk_db]
    log.debug(f"Chunks available for multipart upload: {chunks}")

    uploader = ResourceCloudStorage({})
    try:
        log.debug("Retrieving S3 object.")
        obj = uploader.container.get_object(upload.name)
        log.debug("Complete S3 object already exists, deleting...")
        obj.delete()
    except Exception as e:
        log.debug(f"Error retrieving pre-exiting S3 record: {e}")
        log.debug("Proceeding with multipart commit...")
        pass
    log.debug(
        "Committing multipart object with params: "
        f"container={uploader.container} | "
        f"object_name={upload.name} | "
        f"upload_id={upload_id} | "
        f"chunks={chunks}"
    )
    uploader.driver._commit_multipart(
        container=uploader.container,
        object_name=upload.name,
        upload_id=upload_id,
        chunks=chunks,
    )
    upload.delete()
    upload.commit()
    if chunk_db:
        if chunk_db.first() is not None:
            log.debug("Deleting multipart chunk records from DB")
            chunk_db.delete()
            chunk_db.commit()

    s3_location = (
        f"https://{uploader.driver_options['host']}/"
        f"{uploader.container_name}/{upload.name}"
    )
    log.debug(f"S3 upload location: {s3_location}")

    if save_action and save_action == "go-metadata":
        try:
            res_dict = toolkit.get_action("resource_show")(
                context.copy(), {"id": data_dict.get("id")}
            )
            pkg_dict = toolkit.get_action("package_show")(
                context.copy(), {"id": res_dict["package_id"]}
            )
            toolkit.get_action("resource_patch")(
                dict(context.copy()),
                dict(id=data_dict["id"], last_modified=datetime.datetime.now()),
            )
            if pkg_dict["state"] == "draft":
                toolkit.get_action("package_patch")(
                    dict(context.copy(), allow_state_change=True),
                    dict(id=pkg_dict["id"], state="active"),
                )
        except Exception as e:
            log.error(e)
    return {"commited": True, "url": s3_location}


def abort_multipart(context, data_dict):
    h.check_access("cloudstorage_abort_multipart", data_dict)
    id = toolkit.get_or_bust(data_dict, ["id"])
    uploader = ResourceCloudStorage({})

    resource_uploads = MultipartUpload.resource_uploads(id)

    log.debug("abort_multipart package id={0}".format(id))

    print(resource_uploads)

    aborted = []
    for upload in resource_uploads:
        log.debug("abort_multipart upload id={0}".format(upload.id))
        _delete_multipart(upload, uploader)

        aborted.append(upload.id)

    model.Session.commit()

    return aborted


def clean_multipart(context, data_dict):
    """Clean old multipart uploads.

    :param context:
    :param data_dict:
    :returns: dict with:
        removed - amount of removed uploads.
        total - total amount of expired uploads.
        errors - list of errors raised during deletion. Appears when
        `total` and `removed` are different.
    :rtype: dict

    """

    log.debug("clean_multipart running...")
    h.check_access("cloudstorage_clean_multipart", data_dict)
    uploader = ResourceCloudStorage({})
    delta = _get_max_multipart_lifetime()
    oldest_allowed = datetime.datetime.utcnow() - delta

    uploads_to_remove = (
        model.Session.query(MultipartUpload)
        .filter(MultipartUpload.initiated < oldest_allowed)
        .filter(MultipartUpload.upload_complete == False)
    )

    result = {"removed": 0, "total": uploads_to_remove.count(), "errors": []}

    for upload in uploads_to_remove:
        try:
            _delete_multipart(upload, uploader)
        except toolkit.ValidationError as e:
            result["errors"].append(e.error_summary)
        else:
            result["removed"] += 1

    return result
