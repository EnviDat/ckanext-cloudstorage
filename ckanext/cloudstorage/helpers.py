#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage
from ckan.logic import get_action


def use_secure_urls():
    return all([
        ResourceCloudStorage.use_secure_urls.fget(None),
        # Currently implemented just AWS version
        'S3' in ResourceCloudStorage.driver_name.fget(None),
        'host' in ResourceCloudStorage.driver_options.fget(None),
    ])

def cloudstorage_check_multipart(id):
        context = {}
        return get_action('cloudstorage_check_multipart')(context, {'id': id})