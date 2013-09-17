#! /usr/bin/env python
# -*- coding: utf-8 -*-


# CKANExt-fedmsg -- CKAN extension for sending activity events to fedmsg Bus
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://github.com/etalab/ckanext-fedmsg
#
# This file is part of CKANExt-fedmsg.
#
# CKANExt-fedmsg is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# CKANExt-fedmsg is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""CKAN extension for sending activity events to the Fedora Infrastructure Message Bus

This plugin attachs itself to CKAN hooks and simply republishes information to the fedmsg bus.
"""


from setuptools import setup, find_packages


classifiers = """\
Development Status :: 2 - Pre-Alpha
Environment :: Plugins
Environment :: Web Environment
Intended Audience :: Developers
Intended Audience :: System Administrators
License :: OSI Approved :: GNU Affero General Public License v3
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
"""

doc_lines = __doc__.split('\n')

version = '0.1'


setup(
    name = 'ckanext-fedmsg',
    version = version,

    author = 'Emmanuel Raviart',
    author_email = 'emmanuel@raviart.com',
    classifiers = [classifier for classifier in classifiers.split('\n') if classifier],
    description = doc_lines[0],
    keywords = 'ckan extension fedmsg plugin',
    license = 'http://www.fsf.org/licensing/licenses/agpl-3.0.html',
    long_description = '\n'.join(doc_lines[2:]),
    url = 'https://www.github.com/etalab/ckanext-fedmsg',

    entry_points = """
        [ckan.plugins]
        fedmsg = ckanext.fedmsg.plugins:FedmsgPlugin
        """,
    include_package_data = True,
    install_requires = [
        'ckan',
        'fedmsg',
        ],
    namespace_packages = ['ckanext'],
    packages = find_packages(exclude = ['ez_setup']),
    zip_safe = False,
    )
