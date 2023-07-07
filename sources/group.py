import logging as log
import time

import sources.data_processing as proc
import sources.data_retrieval as ret
import sources.file_handler as fh


class Group:
    __slots__ = ('group_id', 'name', 'date_added', 'language', 'account', 'active', 'telethon_group',
                 'messages_count', 'message_services', 'participants', 'participants_count', 'female', 'male',
                 'unknown', 'female_percentage', 'male_percentage', 'hourly_activity', 'daily_activity')

    def __init__(self, telethon_group, account):
        self.group_id = telethon_group.id
        self.name = telethon_group.name
        self.date_added = fh.TODAY
        self.language = ''
        self.account = account
        self.active = True

        self.telethon_group = telethon_group
        self.message_services = None
        self.messages_count = 0

        self.participants = None
        self.participants_count = telethon_group.entity.participants_count
        self.female = 0
        self.male = 0
        self.unknown = 0
        self.female_percentage = 0
        self.male_percentage = 0

        self.hourly_activity = None
        self.daily_activity = None

    def __eq__(self, other):
        return self.group_id == other.group_id

    def __hash__(self):
        return hash(('group_id', self.group_id))

    async def run(self, week_datetime, is_present, words_to_ignore, words_of_interest):
        log.info(f'Crawling for {self.group_id}..')
        week = week_datetime.strftime('%Y-%m-%d')
        messages, self.message_services = await ret.get_weekly_messages_from_group(self, week_datetime)
        time.sleep(30)
        if is_present:
            self.participants = await ret.get_participants(self)
            time.sleep(30)
            self.female, self.male, self.unknown, self.female_percentage, self.male_percentage = proc.estimate_gender_distribution(
                self.participants, fh.read_first_names())

        self.messages_count = len(messages)
        self.calculate_activity(messages, week)
        self.calculate_frequencies(messages, week, words_to_ignore, words_of_interest)

    def calculate_activity(self, messages, week):
        self.hourly_activity = proc.count_hourly_activity(messages)
        self.daily_activity = proc.count_daily_activity(messages)

        fh.write_activity_bar_chart_group(self.hourly_activity, self.language, week, self.group_id, 'Hourly-Activity')
        fh.write_daily_activity_bar_group(self.daily_activity, self.language, week, self.group_id, 'Daily-Activity')

    def calculate_frequencies(self, messages, week, words_to_ignore, words_of_interest):
        sentences = proc.filter_and_split_messages(messages, words_to_ignore)
        word_single_frequency = proc.calculate_single_word_frequencies(sentences)
        word_pair_frequency = proc.calculate_word_pairs_frequencies(sentences, words_of_interest)

        fh.write_csv_group(word_single_frequency, self.language, week, self.group_id, 'Word-Frequencies')
        fh.write_csv_group(word_pair_frequency, self.language, week, self.group_id,
                           'Words_of_Interest-Pair-Frequencies')

        filtered_word_single_frequency = proc.filter_single_word_frequencies(word_single_frequency, words_of_interest)

        fh.write_csv_group(filtered_word_single_frequency, self.language, week, self.group_id,
                           'Words_of_Interest-Frequencies')

    def get_overview(self):
        return [self.name, self.group_id, self.participants_count, self.messages_count, self.female, self.male,
                self.unknown, self.female_percentage, self.male_percentage]

    def get_participant_joined(self):
        return proc.participant_joined(self.message_services)

    def get_word_single_frequency(self, week):
        return fh.read_frequency_csv_group(self.language, week, self.group_id, 'Word-Frequencies')

    def get_word_pair_frequency(self, week):
        return fh.read_frequency_csv_group(self.language, week, self.group_id, 'Word-Pair-Frequencies')

    def get_filtered_word_single_frequency(self, week):
        return fh.read_frequency_csv_group(self.language, week, self.group_id, 'Words_of_Interest-Frequencies')

    def get_filtered_word_pair_frequency(self, week):
        return fh.read_frequency_csv_group(self.language, week, self.group_id, 'Words_of_Interest-Pair-Frequencies')

    def get_daily_activity(self, week):
        return fh.read_daily_activity_csv_group(self.language, week, self.group_id)

    def get_hourly_activity(self, week):
        return fh.read_hourly_activity_csv_group(self.language, week, self.group_id)
