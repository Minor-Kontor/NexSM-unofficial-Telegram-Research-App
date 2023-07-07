import calendar
import csv
import json
import logging as log
import os
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from wordcloud import WordCloud

TODAY = datetime.today().strftime('%Y-%m-%d')
ROOT = Path(__file__).parent.parent.absolute()
INPUT_PATH = f'{ROOT}/inputs'
OUTPUTS_PATH = f'{ROOT}/outputs'

LOG_PATH = f'{ROOT}/logs/{datetime.now().strftime("%Y-%m-%d-%H-%M")}_log.txt'
SESSION_PATH = f'{ROOT}/sessions'


def read_first_names():
    with open(f'{INPUT_PATH}/first_names.json', encoding='utf8') as file:
        first_names = json.load(file)

        return first_names


def read_words_of_interest(language: str) -> list:
    woi = []

    p, exists = get_path(f'{INPUT_PATH}/{language}/words-of-interest.csv')
    if exists:
        woi_csv = pd.read_csv(p)
        woi += list(woi_csv['word'])

    p, exists = get_path(f'{INPUT_PATH}/words-of-interest.csv')
    if exists:
        woi_csv = pd.read_csv(p)
        woi += list(woi_csv['word'])

    return woi


def read_words_to_ignore(language: str) -> list:
    wti = []

    p, exists = get_path(f'{INPUT_PATH}/{language}/words-to-ignore.csv')
    if exists:
        wti += read_csv_as_list(p)

    p, exists = get_path(f'{INPUT_PATH}/words-to-ignore.csv')
    if exists:
        wti += read_csv_as_list(p)

    return wti


def read_csv_as_list(path):
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        return [''.join(line) for line in reader]


def write_csv_total(df: pd.DataFrame, language, title: str, index=False):
    write_csv(df, f'{OUTPUTS_PATH}/{language}/{title}', index)


def write_csv_weekly(df: pd.DataFrame, language, week: str, title: str, index=False):
    write_csv(df, f'{OUTPUTS_PATH}/{language}/{week}/{title}', index)


def write_csv_group(df: pd.DataFrame, language, week: str, group_id: int, title: str, index=False):
    write_csv(df, f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/{title}', index)


def write_csv(df: pd.DataFrame, path: str, index=False):
    p, _ = get_path(f'{path}.csv')
    df.to_csv(p, index=index, header=True)


def read_daily_activity_csv_group(language: str, week: str, group_id: int):
    df = read_csv(f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/Daily-Activity')
    if df is None:
        return {}
    df.set_index('weekday', inplace=True)
    df.drop('day', axis=1, inplace=True)

    return df.to_dict()['message']


def read_hourly_activity_csv_group(language: str, week: str, group_id: int):
    df = read_csv(f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/Hourly-Activity')
    if df is None:
        return {}
    df.set_index('hour', inplace=True)

    return df.to_dict()['message']


def read_frequency_csv_group(language: str = None, week: str = '', group_id: int = -1, title: str = ''):
    df = read_csv(f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/{title}')

    if df is None:
        return {}
    if 'word-of-interest' in df.columns:
        df.set_index(['word-of-interest', 'word'], inplace=True)
    else:
        df.set_index('word', inplace=True)

    return df.to_dict()['frequency']


def read_csv(path: str):
    p, exists = get_path(f'{path}.csv')

    if exists:
        return pd.read_csv(p)

    log.warning(f'Did not find a csv that was expected to be found: {path}')
    return None


def update_total_overview(language, total_participants, accessible_participants, total_groups, unique_participants,
                          estimated_female_participants, estimated_male_participants, estimated_unknown_participants,
                          estimated_female_participants_p, estimated_male_participants_p):
    p, exists = get_path(f'{OUTPUTS_PATH}/{language}/Total-Overview.csv')

    if exists:
        df = pd.read_csv(p)
    else:
        df = pd.DataFrame(
            columns=['date', 'total-participants', 'accessible_participants', 'total-groups', 'unique-participants',
                     'estimated-female-participants', 'estimated-male-participants',
                     'estimated-unknown-participants', 'estimated-female-participants %',
                     'estimated-male-participants %'])

    df.loc[-1] = [TODAY, total_participants, accessible_participants, total_groups, unique_participants,
                  estimated_female_participants,
                  estimated_male_participants, estimated_unknown_participants, estimated_female_participants_p,
                  estimated_male_participants_p]
    df.sort_values('date', ascending=False)

    df.to_csv(p, index=False, header=True)


def update_groups(groups: list):
    groups_list = [[g.group_id, g.name, TODAY, '', g.account, True] for g in groups]
    new_groups_df = pd.DataFrame(groups_list, columns=['id', 'name', 'date-added', 'language', 'account', 'active'])
    p, exists = get_path(f'{INPUT_PATH}/groups.csv')

    if exists:
        groups_df = pd.read_csv(p, keep_default_na=False)
        groups_df = pd.concat([groups_df, new_groups_df]).drop_duplicates(subset='id', keep='first')
    else:
        groups_df = new_groups_df

    groups_df['language'] = groups_df['language'].fillna('').astype(str).str.lower()

    write_csv(groups_df, f'{INPUT_PATH}/groups')

    groups_df_with_index = groups_df.set_index('id')
    for g in groups:
        g_series = groups_df_with_index.loc[g.group_id]
        g.language = g_series['language']
        if g.language is None or g.language == '':
            g.language = 'unknown'
        g.active = g_series['active']

    return groups


def write_daily_activity_bar_group(df: pd.DataFrame, language, week: str, group_id: int, title: str):
    df['day'] = df.index.map(lambda d: calendar.day_name[d])
    write_bar_chart(df, f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/{title}', title, 'day', 'message')
    df.drop('day', axis=1, inplace=True)


def write_daily_activity_bar_week(df: pd.DataFrame, language, week: str, title: str):
    df['day'] = df.index.map(lambda d: calendar.day_name[d])
    write_bar_chart(df, f'{OUTPUTS_PATH}/{language}/{week}/{title}', title, 'day', 'message')
    df.drop('day', axis=1, inplace=True)


def write_activity_bar_chart_group(df: pd.DataFrame, language, week: str, group_id: int, title: str):
    write_bar_chart(df, f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/{title}', title)


def write_activity_bar_chart_week(df: pd.DataFrame, language, week: str, title: str):
    write_bar_chart(df, f'{OUTPUTS_PATH}/{language}/{week}/{title}', title)


def write_bar_chart(df: pd.DataFrame, path: str, title=None, x=None, y=None):
    if df.empty:
        return

    df.plot.bar(x, y)
    plt.title(title)
    plt.xticks(rotation=45, fontweight='light', fontsize='x-small')

    p, _ = get_path(f'{path}.png')
    plt.savefig(p)
    p, _ = get_path(f'{path}.csv')
    df.to_csv(p)


def write_plot_total(x, y, language, title=None, xlabel=None, ylabel=None):
    write_plot(x, y, f'{OUTPUTS_PATH}/{language}/{title}', title, xlabel, ylabel)


def write_weekly_joined_participants(dates, language, week):
    dates.sort_values('date', ascending=True, inplace=True, ignore_index=True)

    write_csv(dates['date'], f'{OUTPUTS_PATH}/{language}/{week}/Weekly-Joins')
    write_plot_weekly(dates['date'], dates.index, language, week, 'Weekly-Joins', 'date',
                      'Participants joined')


def write_plot_weekly(x, y, language, week: str, title=None, xlabel=None, ylabel=None):
    write_plot(x, y, f'{OUTPUTS_PATH}/{language}/{week}/{title}', title, xlabel, ylabel)


def write_plot_group(x, y, language, week: str, group_id: int, title=None, xlabel=None, ylabel=None):
    write_plot(x, y, f'{OUTPUTS_PATH}/{language}/{week}/group_{group_id}/{title}', title, xlabel, ylabel)


def write_plot(x, y, path: str, title=None, xlabel=None, ylabel=None):
    fig = plt.figure()
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=90, fontweight='light', fontsize='x-small')
    plt.plot(x, y)

    p, _ = get_path(f'{path}.png')
    fig.savefig(p)

    plt.close(fig)


def plot_wordcloud(frequencies: pd.DataFrame):
    d = {}
    for word, count in frequencies.values:
        d[word] = count

    wordcloud = WordCloud()
    wordcloud.generate_from_frequencies(frequencies=d)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()


def write_weekly_network_graph(participants: pd.DataFrame, language, week):
    write_csv_weekly(participants[['id', 'group_id']], language, week, 'Group-Participant-Network')


def write_network_graph(df: pd.DataFrame, source_col, destination_col, title: str, path: str):
    plt.figure(figsize=(12, 12))

    g = nx.from_pandas_edgelist(df, source=source_col, target=destination_col)
    layout = nx.spring_layout(g, iterations=50)

    unique_destinations = list(df[destination_col].unique())
    unique_sources = list(df[source_col].unique())
    destination_degrees = [g.degree(dest) * 80 for dest in unique_destinations]

    nx.draw_networkx_nodes(g, layout, nodelist=unique_destinations, node_size=destination_degrees,
                           node_color='lightblue')
    nx.draw_networkx_nodes(g, layout, nodelist=unique_sources, node_color='#cccccc', node_size=10)

    destination_labels = dict(zip(unique_destinations, unique_destinations))
    nx.draw_networkx_labels(g, layout, labels=destination_labels)

    plt.axis('off')
    plt.title(title)

    p, _ = get_path(f'{path}.png')
    plt.savefig(p)


def create_input_directories(languages: list):
    df_woi = pd.DataFrame(columns=['word', 'translation'])
    df_wti = pd.DataFrame(columns=['word', 'translation'])
    languages.append('unknown')
    for lang in languages:
        p, exists = get_path(f'{INPUT_PATH}/{lang}/words-of-interest.csv')

        if not exists:
            df_woi.to_csv(p, index=False, header=True)

        p, exists = get_path(f'{INPUT_PATH}/{lang}/words-to-ignore.csv')

        if not exists:
            df_wti.to_csv(p, index=False, header=True)

    # creating global files
    p, exists = get_path(f'{INPUT_PATH}/words-of-interest.csv')

    if not exists:
        df_woi.to_csv(p, index=False, header=True)

    p, exists = get_path(f'{INPUT_PATH}/words-to-ignore.csv')

    if not exists:
        df_wti.to_csv(p, index=False, header=True)


def get_all_words_of_interest():
    languages = [lang for lang in os.listdir(INPUT_PATH) if os.path.isdir(f'{INPUT_PATH}/{lang}')]
    words_of_interests = {lang: read_words_of_interest(lang) for lang in languages}

    return words_of_interests


def get_all_word_frequencies(languages: list):
    def iterate_output(language):
        parent = True
        for (path, sub_directories, files) in os.walk(f'{OUTPUTS_PATH}/{language}'):
            if parent:
                parent = False
                continue

            if sub_directories:
                input_file_path = f'{path}/Weekly-Word-Frequencies'
                output_file_path = f'{path}/Weekly-Words_of_Interest-Frequencies'
            else:
                input_file_path = f'{path}/Word-Frequencies'
                output_file_path = f'{path}/Words_of_Interest-Frequencies'

            yield read_frequency(path=input_file_path), output_file_path

    word_frequencies = {}

    for language in languages:
        word_frequencies[language] = iterate_output(language)

    return word_frequencies


def read_frequency(path: str):
    df = read_csv(path)

    if df is None:
        df = pd.DataFrame(columns=['word', 'frequency'])

    return df


def get_api_values() -> dict:
    path = f'{INPUT_PATH}/api_values.csv'
    p, exists = get_path(path)

    if exists:
        api_values = pd.read_csv(p, index_col='label')

        api_values.index = api_values.index.str.strip()
        api_values['api_hash'] = api_values['api_hash'].str.strip()
        return api_values.to_dict(orient='index')

    return None


def get_path(path: str):
    p = Path(path)
    exists = True
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        exists = False

    return p, exists


def get_session(name: str):
    path = f'{SESSION_PATH}/{name}.session'
    _, _ = get_path(path)

    return path


def setup_log():
    p, _ = get_path(LOG_PATH)
    log.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s', level=log.INFO,
                    handlers=[log.FileHandler(p, 'w', 'utf-8'), log.StreamHandler(sys.stdout)])
