import datetime
import logging as log
import time

import pandas as pd
import pytz
import telethon
from telethon import TelegramClient

from sources.enums import Action

clients: {TelegramClient} = {}
local_tz = 'Europe/Berlin'
local_pytz = pytz.timezone(local_tz)


async def login(account: str, session, api_id: int, api_hash: str):
    log.info(f'Attempting login for {account}..')
    clients[account] = TelegramClient(session, api_id, api_hash)
    await clients[account].start()

    return clients[account]


async def disconnect(account: str):
    await clients[account].disconnect()


async def get_groups(account: str):
    dialogs = await clients[account].get_dialogs()
    groups = filter(lambda d: d.is_group, dialogs)
    return list(groups)


async def get_participants(group):
    return pd.DataFrame(
        [[group.group_id, m.id, m.first_name] for m in
         await clients[group.account].get_participants(group.telethon_group)],
        columns=['group_id', 'id', 'first'])


def get_week(date: str = None):
    if date is not None:
        monday = local_pytz.localize(datetime.datetime.strptime(f'{date} 0:0:0', '%Y-%m-%d %H:%M:%S'),
                                     is_dst=True)
        monday = monday - datetime.timedelta(days=monday.weekday())
    else:
        monday = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        monday = monday - datetime.timedelta(days=(monday.weekday() + 7))

    return monday


async def get_weekly_messages_from_group(group, begin_date):
    begin_date_utc = begin_date.astimezone(pytz.utc)
    end_date = begin_date_utc + datetime.timedelta(days=7)

    try:
        pre_first_msg = await clients[group.account].get_messages(group.telethon_group, offset_date=begin_date_utc,
                                                                  limit=1)
        pre_first_msg = pre_first_msg[0]
        time.sleep(5)
        first_msg = (await clients[group.account].get_messages(group.telethon_group, min_id=pre_first_msg.id, limit=1,
                                                               reverse=True))[0]
        time.sleep(5)
        last_msg = (await clients[group.account].get_messages(group.telethon_group, offset_date=end_date, limit=1))[0]
    except IndexError:
        log.warning(
            f'No messages received during the week from {begin_date_utc} to {end_date} for group: {group.name}.')
        messages = pd.DataFrame(columns=['date', 'message'])
        messages['date'] = pd.to_datetime(messages['date'])
        message_services = pd.DataFrame(columns=['date', 'action'])
        message_services['date'] = pd.to_datetime(message_services['date'])
        return messages, message_services

    chat_history = await clients[group.account].get_messages(group.telethon_group, min_id=first_msg.id - 1,
                                                             max_id=last_msg.id + 1)

    messages = filter(lambda m: isinstance(m, telethon.tl.patched.Message), chat_history)
    message_services = filter(lambda m: isinstance(m, telethon.tl.patched.MessageService), chat_history)
    messages = pd.DataFrame([[m.date, m.message] for m in messages],
                            columns=['date', 'message'])
    temp = []
    for m in message_services:
        if isinstance(m.action, telethon.tl.types.MessageActionChatJoinedByRequest):
            action = Action.join
        elif isinstance(m.action, telethon.tl.types.MessageActionChatAddUser):
            action = Action.join
        elif isinstance(m.action, telethon.tl.types.MessageActionChatJoinedByLink):
            action = Action.join
        elif isinstance(m.action, telethon.tl.types.MessageActionChatDeleteUser):
            action = Action.leave
        else:
            action = m.action.stringify()
        temp.append([m.date, action])
    message_services = temp
    message_services = pd.DataFrame(message_services, columns=['date', 'action'])

    messages['date'] = pd.to_datetime(messages['date'], utc=True).dt.tz_convert(local_tz)
    message_services['date'] = pd.to_datetime(message_services['date'], utc=True).dt.tz_convert(local_tz)

    return messages, message_services
