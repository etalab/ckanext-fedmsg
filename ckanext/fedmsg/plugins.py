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


import logging
import socket
import traceback

from biryani1 import baseconv as conv
from ckan import model, plugins
import fedmsg


fedmsg_config = None
log = logging.getLogger(__name__)


class FedmsgPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.ISession, inherit = True)

    def before_commit(self, session):
        # Code inspired from ckan.model.modification.DomainObjectModificationExtension.
        session.flush()
        if not hasattr(session, '_object_cache'):
            return
        object_cache = session._object_cache

        context = dict(
            # active = True,
            api_version = 3,
            # include_private_packages = False
            # keep_sensitive_data = False,
            model = model,
            session = model.Session,
            # user = None,  # Package creation (and delete) fails when user is set to None.
            )
        group_command_by_id = {}
        organization_command_by_id = {}
        package_command_by_id = {}
        related_command_by_id = {}
        tag_command_by_id = {}
        user_command_by_id = {}
        for action, instances in (
                ('create', object_cache['new']),
                ('delete', object_cache['deleted']),
                ('update', object_cache['changed']),
                ):
            for instance in instances:
                if instance.__class__.__name__.endswith('Revision'):
                    # Ignore changes on revisions.
                    continue
                if isinstance(instance, (
                        model.ActivityDetail,
                        model.GroupRole,
                        model.PackageRole,
                        model.Revision,
                        model.RoleAction,
                        model.SystemInfo,
                        model.SystemRole,
                        #model.TaskStatus,
                        model.TrackingSummary,
                        model.UserFollowingUser,
                        model.UserFollowingDataset,
                        model.UserFollowingGroup,
                        model.UserObjectRole,
                        #model.Vocabulary,
                        )):
                    continue
                if isinstance(instance, model.Activity):
                    # Hack that uses activity to detect when a Related has been created and to retrieve its dataset.
                    # Because when a related is created, its dataset is not known when before_commit is called.
                    if action == 'create' and instance.activity_type == u'new related item':
                        related_json = instance.data['related']
                        related_json['dataset_id'] = instance.data['dataset']['id']
                        try:
                            fedmsg.publish(
                                modname = fedmsg_config['modname'],
                                msg = related_json,
                                topic = '{}.{}'.format('related', action),
                                )
                        except:
                            traceback.print_exc()
                            raise
                    # Ignore activity changes.
                    continue
                elif isinstance(instance, model.Group):
                    if instance.is_organization:
                        add_command(organization_command_by_id, action, instance)
                    else:
                        add_command(group_command_by_id, action, instance)
                elif isinstance(instance, (model.GroupExtra, model.Member)):
                    group = instance.group
                    if group is not None:
                        if group.is_organization:
                            add_command(organization_command_by_id, 'update', group)
                        else:
                            add_command(group_command_by_id, 'update', group)
                elif isinstance(instance, model.Package):
                    add_command(package_command_by_id, action, instance)
                elif isinstance(instance, (model.PackageExtra, model.PackageTag)):
                    add_command(package_command_by_id, 'update', instance.package)
                elif isinstance(instance, model.PackageRelationship):
                    add_command(package_command_by_id, 'update', instance.object)
                    add_command(package_command_by_id, 'update', instance.subject)
                elif isinstance(instance, model.Rating):
                    add_command(package_command_by_id, 'update', instance.package)
                    add_command(user_command_by_id, 'update', instance.user)
                elif isinstance(instance, model.Related):
                    # Note: "create" action is handled using Activity, because there is no way to retrieve the related
                    # dataset at creation time (RelatedDataset doesn't exist yet).
                    if action != 'create':
                        add_command(related_command_by_id, action, instance)
#                elif isinstance(instance, model.RelatedDataset):
#                    add_command(package_command_by_id, 'update', instance.dataset)
                elif isinstance(instance, model.Resource):
                    resource_group = instance.resource_group
                    if resource_group is not None:
                        add_command(package_command_by_id, 'update', resource_group.package)
                elif isinstance(instance, model.ResourceGroup):
                    add_command(package_command_by_id, 'update', instance.package)
                elif isinstance(instance, model.Tag):
                    add_command(tag_command_by_id, action, instance)
                elif isinstance(instance, model.User):
                    add_command(user_command_by_id, action, instance)
                else:
                    log.debug('IMapper not handled : {} - {}'.format(action, instance))

        # Note: Order of items in "for" instructions is important.
        for action in ('create', 'update'):
            for kind, command_by_id, to_json in (
                    ('tag', tag_command_by_id, plugins.toolkit.get_action('tag_show')),
                    ('group', group_command_by_id, plugins.toolkit.get_action('group_show')),
                    ('organization', organization_command_by_id, plugins.toolkit.get_action('organization_show')),
                    ('related', related_command_by_id, related_show),
                    ('user', user_command_by_id, plugins.toolkit.get_action('user_show')),
                    ('package', package_command_by_id, plugins.toolkit.get_action('package_show')),
                    ):
                for command_action, instance in command_by_id.itervalues():
                    if command_action != action:
                        continue
                    try:
                        fedmsg.publish(
                            modname = fedmsg_config['modname'],
                            msg = to_json(context, dict(id = instance.id)),
                            topic = '{}.{}'.format(kind, action),
                            )
                    except:
                        traceback.print_exc()
                        raise
        for action in ('delete',):
            for kind, command_by_id in (
                    ('package', package_command_by_id),
                    ('user', user_command_by_id),
                    ('related', related_command_by_id),
                    ('organization', organization_command_by_id),
                    ('group', group_command_by_id),
                    ('tag', tag_command_by_id),
                    ):
                for command_action, instance in command_by_id.itervalues():
                    if command_action != action:
                        continue
                    try:
                        fedmsg.publish(
                            modname = fedmsg_config['modname'],
                            msg = dict(id = instance.id),
                            topic = '{}.{}'.format(kind, action),
                            )
                    except:
                        traceback.print_exc()
                        raise

    def configure(self, config):
        hostname = socket.gethostname().split('.')[0]

        global fedmsg_config
        fedmsg_config = conv.check(conv.struct(dict(
            environment = conv.pipe(
                conv.empty_to_none,
                conv.test_in(['dev', 'prod', 'stg']),
                conv.default('dev'),
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
                conv.not_none,
                ),
            )))(dict(
                (key[len('fedmsg.'):], value)
                for key, value in config.iteritems()
                if key.startswith('fedmsg.')
                ))

        #fedmsg.init(**fedmsg_config)
        fedmsg.init(active = True, name = 'relay_inbound', **dict(
            (key, value)
            for key, value in fedmsg_config.iteritems()
            if key != 'name' and value is not None
            ))


def add_command(command_by_id, action, instance):
    assert action in ('create', 'delete', 'update'), action
    if instance is None:
        return
    if isinstance(instance, model.Package) and instance.state != 'active' and action != 'delete':
        action = 'delete'
    id = instance.id
    assert id is not None
    command = command_by_id.get(id)
    if command is None:
        command_by_id[id] = [action, instance]
    elif action in ('create', 'delete'):
        if command[0] == 'update':
            command[0] = action
        else:
            assert command[0] == action


def related_show(context, data_dict):
    related_dict = plugins.toolkit.get_action('related_show')(context, data_dict)
    model = context['model']
    related_dataset = model.Session.query(model.RelatedDataset).filter(
        model.RelatedDataset.related_id == data_dict['id'],
        model.RelatedDataset.status == u'active',
        ).first()
    if related_dataset is not None:
        related_dict['dataset_id'] = related_dataset.dataset_id
    return related_dict
