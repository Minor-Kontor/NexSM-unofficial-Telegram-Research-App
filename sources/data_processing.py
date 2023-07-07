import re
from collections import Counter
from itertools import chain, combinations, product

import emoji
import pandas as pd

from sources.enums import Gender, Action

specials = re.escape('!@#$%^&*()[]{};:,./<>?\|`~-=_+')
specials = f'[{specials}]'


def filter_and_split_messages(messages: pd.DataFrame, words_to_ignore: list = None):
    words = [emoji.replace_emoji(re.sub(specials, ' ', m), ' ').lower().split() for m in messages['message']]
    emojis = [emoji.distinct_emoji_list(m) for m in messages['message']]
    words_emojis = [w + em for w, em in zip(words, emojis)]
    if words_to_ignore is not None:
        words_to_ignore = [w.lower() for w in words_to_ignore]
        words_emojis = list(map(lambda sentence: [w for w in sentence if w not in words_to_ignore], words_emojis))

    return words_emojis


def calculate_word_pairs_frequencies(sentences: list, words_of_interest=None):
    if not words_of_interest:
        return pd.DataFrame(columns=['word-of-interest', 'word', 'frequency'])

    def combinations_helper(sentence):
        sentence_set = set(sentence)
        woi = []
        rest = []

        for word in sentence_set:
            if word in words_of_interest:
                woi.append(word)
            else:
                rest.append(word)

        woi = sorted(woi)
        return chain(product(woi, rest), combinations(woi, 2))

    tuples = chain.from_iterable(map(combinations_helper, sentences))
    counter_pairs = Counter(tuples)

    frequencies_pairs = pd.DataFrame([(w1, w2, f) for ((w1, w2), f) in counter_pairs.items()],
                                     columns=['word-of-interest', 'word', 'frequency'])
    frequencies_pairs.sort_values(['frequency', 'word-of-interest', 'word'], ascending=[False, True, True],
                                  inplace=True)

    return frequencies_pairs


def calculate_single_word_frequencies(sentences: list):
    counter_singles = Counter(chain.from_iterable(sentences))

    frequencies_singles = pd.DataFrame({'word': counter_singles.keys(), 'frequency': counter_singles.values()})
    frequencies_singles.sort_values(['frequency', 'word'], ascending=[False, True], inplace=True)

    return frequencies_singles


def filter_single_word_frequencies(frequencies: pd.DataFrame, words_of_interest):
    filtered_frequencies = frequencies[
        frequencies['word'].apply(lambda w: w in [w.lower() for w in words_of_interest])].copy()

    if filtered_frequencies.empty:
        filtered_frequencies = pd.DataFrame(columns=['word', 'frequency'])

    return filtered_frequencies


def filter_word_pairs_frequencies(frequencies: pd.DataFrame, words_of_interest: list):
    woi = list(map(str.lower, words_of_interest))

    def helper(t: tuple):
        return t[0] in woi or t[1] in woi

    filtered_frequencies = frequencies[frequencies['word'].apply(helper)].copy()

    if filtered_frequencies.empty:
        filtered_frequencies = pd.DataFrame(columns=['word', 'frequency'])

    return filtered_frequencies


def combine_word_frequencies(frequencies):
    combined_frequencies = sum((Counter(f) for f in frequencies), Counter())
    frequencies = pd.DataFrame(combined_frequencies.most_common(), columns=['word', 'frequency'])
    frequencies.sort_values(['frequency', 'word'], ascending=[False, True], inplace=True)

    return frequencies


def combine_pair_frequencies(frequencies):
    combined_frequencies = sum((Counter(f) for f in frequencies), Counter())
    frequencies = pd.DataFrame([(w1, w2, f) for ((w1, w2), f) in combined_frequencies.most_common()],
                               columns=['word-of-interest', 'word', 'frequency'])
    frequencies.sort_values(['frequency', 'word-of-interest', 'word'], ascending=[False, True, True], inplace=True)

    return frequencies


def combine_daily_activity(daily_activities):
    combined_activity = sum((Counter(f) for f in daily_activities), Counter())
    frequencies = pd.DataFrame(combined_activity.items(), columns=['weekday', 'message'])
    frequencies.sort_values('weekday', ascending=True, inplace=True)
    frequencies.set_index('weekday', inplace=True)

    return frequencies


def combine_hourly_activity(hourly_activities):
    combined_activity = sum((Counter(f) for f in hourly_activities), Counter())
    frequencies = pd.DataFrame(combined_activity.items(), columns=['hour', 'message'])
    frequencies.sort_values('hour', ascending=True, inplace=True)
    frequencies.set_index('hour', inplace=True)

    return frequencies


def estimate_gender_distribution(participants, first_names) -> (int, int, int, float, float):
    def helper(first):
        if first is None:
            return Gender.unknown
        is_female = first in first_names['female']
        is_male = first in first_names['male']

        if is_female & is_male:
            return Gender.unknown
        if is_female:
            return Gender.female
        if is_male:
            return Gender.male

        return Gender.unknown

    participants['gender'] = participants['first'].apply(helper)

    return calc_gender_distribution(participants)


def count_hourly_activity(messages: pd.DataFrame):
    daily_activity = messages.groupby(messages['date'].dt.hour).agg({'message': 'count'})
    daily_activity.index.names = ['hour']
    daily_activity.sort_index(inplace=True)

    return daily_activity


def count_daily_activity(messages: pd.DataFrame):
    weekly_activity = messages.groupby(messages['date'].dt.weekday).agg({'message': 'count'})
    weekly_activity.index.names = ['weekday']
    weekly_activity.sort_index(inplace=True)

    return weekly_activity


def calc_gender_distribution(participants):
    female = len(participants[participants['gender'] == Gender.female])
    male = len(participants[participants['gender'] == Gender.male])
    unknown = len(participants[participants['gender'] == Gender.unknown])
    if female + male == 0:
        percentage_female = 0
        percentage_male = 0
    else:
        percentage_female = female / (female + male)
        percentage_male = male / (female + male)

    return female, male, unknown, percentage_female, percentage_male


def participant_joined(message_services):
    if message_services is None:
        return pd.DataFrame(columns=['date'])
    return message_services.loc[message_services['action'] == Action.join, ['date']].copy()


def estimate_participant_count_over_time(message_services):
    message_services['est_participant_count'] = message_services.loc[::-1, 'action'].apply(
        lambda a: 1 if a == Action.join else (-1 if a == Action.leave else 0)).cumsum()[::-1]

    return message_services[
        (message_services['action'] == Action.join) | (message_services['action'] == Action.leave)].copy()
