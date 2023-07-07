import logging as log

import pandas as pd
from guppy import hpy

import sources.data_processing as proc
import sources.file_handler as fh
from sources.group import Group


class Crawler:
    def __init__(self, language: str, week_datetime: str, is_present: bool = True):
        self.language = language
        self.is_present = is_present
        self.week = week_datetime.strftime('%Y-%m-%d')
        self.week_datetime = week_datetime
        self.groups = []
        self.words_of_interest = fh.read_words_of_interest(self.language)
        self.words_to_ignore = fh.read_words_to_ignore(self.language)

        self.total_participants_count = 0
        self.accessible_participants_count = 0
        self.unique_participants_count = 0
        self.female = 0
        self.male = 0
        self.unknown = 0
        self.female_percentage = 0
        self.male_percentage = 0

    def add_group(self, group: Group):
        self.groups.append(group)

    async def run(self, log_heap=False):
        log.info(f'Crawling for {self.language} a total of {len(self.groups)} groups for week {self.week}..')
        for idx, g in enumerate(self.groups):
            log.info(f'Group {idx + 1} of {len(self.groups)}')
            if log_heap:
                log.info(f'heap information: \n{hpy().heap()}')
            await g.run(self.week_datetime, self.is_present, self.words_to_ignore, self.words_of_interest)

        log.info('Crawled all groups, now creating statistics..')

        self.write_combined_joined()

        if self.is_present:
            self.write_combined_participants()
            self.write_group_overview()

            fh.update_total_overview(self.language, self.total_participants_count, self.accessible_participants_count,
                                     len(self.groups), self.unique_participants_count, self.female, self.male,
                                     self.unknown, self.female_percentage, self.male_percentage)

        self.write_combined_word_frequencies((g.get_word_single_frequency(self.week) for g in self.groups),
                                             'Weekly-Word-Frequencies')
        self.write_combined_word_frequencies((g.get_filtered_word_single_frequency(self.week) for g in self.groups),
                                             'Weekly-Words_of_Interest-Frequencies')
        self.write_combined_pair_frequencies((g.get_filtered_word_pair_frequency(self.week) for g in self.groups),
                                             'Weekly-Words_of_Interest-Pair-Frequencies')

        self.write_combined_daily_activity('Combined-Daily-Activity')
        self.write_combined_hourly_activity('Combined-Hourly-Activity')

        log.info(f'Finished crawling for {self.language} a total of {len(self.groups)} for week {self.week}.')

    def write_group_overview(self):
        group_overview = pd.DataFrame([g.get_overview() for g in self.groups],
                                      columns=['group', 'id', 'participants', 'messages', 'est-female', 'est-male',
                                               'est-unknown', 'est-female %', 'est-male %'])

        self.total_participants_count = group_overview['participants'].sum()
        fh.write_csv_weekly(group_overview, self.language, self.week, 'Group-Overview')

    def write_combined_word_frequencies(self, frequencies, title):
        word_single_frequencies = proc.combine_word_frequencies(frequencies)
        fh.write_csv_weekly(word_single_frequencies, self.language, self.week, title)

    def write_combined_pair_frequencies(self, frequencies, title):
        word_pair_frequencies = proc.combine_pair_frequencies(frequencies)
        fh.write_csv_weekly(word_pair_frequencies, self.language, self.week, title)

    def write_combined_joined(self):
        participant_joined = pd.concat([g.get_participant_joined() for g in self.groups], ignore_index=True)
        fh.write_weekly_joined_participants(participant_joined, self.language, self.week)

        for g in self.groups:
            g.message_services = None

    def write_combined_daily_activity(self, title):
        daily_activities = (g.get_daily_activity(self.week) for g in self.groups)
        combined_daily_activity = proc.combine_daily_activity(daily_activities)

        fh.write_daily_activity_bar_week(combined_daily_activity, self.language, self.week, title)

    def write_combined_hourly_activity(self, title):
        hourly_activities = (g.get_hourly_activity(self.week) for g in self.groups)
        combined_hourly_activity = proc.combine_hourly_activity(hourly_activities)

        fh.write_activity_bar_chart_week(combined_hourly_activity, self.language, self.week, title)

    def write_combined_participants(self):
        all_participants = pd.concat([g.participants for g in self.groups])
        all_participants['id'] = all_participants.groupby('id').ngroup()
        self.accessible_participants_count = len(all_participants)

        fh.write_weekly_network_graph(all_participants, self.language, self.week)

        unique_participants = all_participants.groupby('id').agg(
            {'group_id': list, 'first': 'first', 'gender': 'first'})
        self.unique_participants_count = len(unique_participants)
        self.female, self.male, self.unknown, self.female_percentage, self.male_percentage = proc.calc_gender_distribution(
            unique_participants)

        for g in self.groups:
            g.participants = None
