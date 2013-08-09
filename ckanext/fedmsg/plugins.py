# -*- coding: utf-8 -*-


# CKANExt-fedmsg -- CKAN extension for sending activity events to fedmsg Bus
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://gitorious.org/etalab/ckanext-fedmsg
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


import socket
import time

from biryani1 import baseconv as conv
from ckan import model, plugins
from ckan.lib.dictization import model_dictize
import fedmsg


fedmsg_config = None


class FedmsgPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IMapper, inherit = True)

    last_publication_id = None
    last_publication_time = None
    last_publication_type = None

    def after_delete(self, mapper, connection, instance):
        self.publish('delete', instance)

    def after_insert(self, mapper, connection, instance):
        self.publish('create', instance)

    def after_update(self, mapper, connection, instance):
        self.publish('update', instance)

    def configure(self, config):
        hostname = socket.gethostname().split('.')[0]

        global fedmsg_config
        fedmsg_config = conv.check(conv.struct(dict(
            environment = conv.pipe(
                conv.empty_to_none,
                conv.test_in(['dev', 'prod', 'stg']),
                ),
            modname = conv.pipe(
                conv.empty_to_none,
                conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                conv.default('ckan'),
                ),
            name = conv.pipe(
                conv.empty_to_none,
                conv.default('ckan.{}'.format(hostname)),
                ),
            topic_prefix = conv.pipe(
                conv.empty_to_none,
                conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                ),
            )))(dict(
                (key[len('fedmsg.')], value)
                for key, value in config.iteritems()
                if key.startswith('fedmsg.')
                ))

        #fedmsg.init(**fedmsg_config)
        fedmsg.init(active = True, name = 'relay_inbound', **dict(
            (key, value)
            for key, value in fedmsg_config.iteritems()
            if key != 'name' and value is not None
            ))

    @classmethod
    def publish(cls, action, instance):
        if isinstance(instance, (
                model.Activity,
                model.ActivityDetail,
                #model.GroupRole,
                #model.PackageRole,
                #model.RoleAction,
                model.SystemInfo,
                #model.SystemRole,
                #model.TaskStatus,
                model.TrackingSummary,
                model.UserFollowingUser,
                model.UserFollowingDataset,
                model.UserFollowingGroup,
                #model.UserObjectRole,
                #model.Vocabulary,
                )):
            return
        group = None
        packages = None
        tag = None
        user = None
        if isinstance(instance, model.Group):
            group = instance
        elif isinstance(instance, model.GroupExtra):
            group = instance.group
        elif isinstance(instance, model.Member):
            group = instance.group
        elif isinstance(instance, model.Package):
            packages = [instance]
        elif isinstance(instance, model.PackageExtra):
            package = instance.package
            if package is not None:
                packages = [package]
        elif isinstance(instance, model.PackageRelationship):
            packages = [
                package
                for package in (instance.subject, instance.object)
                if package is not None
                ]
        elif isinstance(instance, model.PackageTag):
            package = instance.package
            if package is not None:
                packages = [package]
        elif isinstance(instance, model.Rating):
            package = instance.package
            if package is not None:
                packages = [package]
            user = instance.user
        elif isinstance(instance, model.Related):
            packages = [
                package
                for package in instance.datasets
                ]
        elif isinstance(instance, model.RelatedDataset):
            package = instance.dataset
            if package is not None:
                packages = [package]
        elif isinstance(instance, model.Resource):
            resource_group = instance.resource_group
            if resource_group is not None:
                package = resource_group.package
                if package is not None:
                    packages = [package]
        elif isinstance(instance, model.ResourceGroup):
            package = instance.package
            if package is not None:
                packages = [package]
        elif isinstance(instance, model.Tag):
            tag = instance
        elif isinstance(instance, model.User):
            user = instance
        else:
            print 'TODO: IMapper {}: {}'.format(action, instance)

        context = dict(
            keep_sensitive_data = True,
            model = model,
            )
        now = time.time()
        if group is not None:
            if cls.last_publication_time is None or cls.last_publication_time + 1 < now \
                    or cls.last_publication_type != 'group' or cls.last_publication_id != group.id:
                fedmsg.publish(
                    topic = '{}.{}'.format('organization' if group.is_organization else 'group', action),
                    modname = fedmsg_config['modname'],
                    msg = model_dictize.group_dictize(group, context),
                    )
            cls.last_publication_id = group.id
            cls.last_publication_time = now
            cls.last_publication_type = 'group'
        if packages:
            for package in packages:
                if cls.last_publication_time is None or cls.last_publication_time + 1 < now \
                        or cls.last_publication_type != 'package' or cls.last_publication_id != package.id:
                    fedmsg.publish(
                        topic = 'package.{}'.format(action),
                        modname = fedmsg_config['modname'],
                        msg = model_dictize.package_dictize(package, context),
                        )
            cls.last_publication_id = package.id
            cls.last_publication_time = now
            cls.last_publication_type = 'package'
        if tag is not None:
            if cls.last_publication_time is None or cls.last_publication_time + 1 < now \
                    or cls.last_publication_type != 'tag' or cls.last_publication_id != tag.id:
                fedmsg.publish(
                    topic = 'tag.{}'.format(action),
                    modname = fedmsg_config['modname'],
                    msg = model_dictize.tag_dictize(tag, context),
                    )
            cls.last_publication_id = tag.id
            cls.last_publication_time = now
            cls.last_publication_type = 'tag'
        if user is not None:
            if cls.last_publication_time is None or cls.last_publication_time + 1 < now \
                    or cls.last_publication_type != 'user' or cls.last_publication_id != user.id:
                fedmsg.publish(
                    topic = 'user.{}'.format(action),
                    modname = fedmsg_config['modname'],
                    msg = model_dictize.user_dictize(user, context),
                    )
            cls.last_publication_id = user.id
            cls.last_publication_time = now
            cls.last_publication_type = 'user'
