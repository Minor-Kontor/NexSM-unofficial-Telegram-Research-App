import argparse
import asyncio
import logging as log
import time
import traceback
from itertools import chain

from tqdm import tqdm

from sources import file_handler as fh, data_retrieval as ret, data_processing as proc
from sources.crawler import Crawler
from sources.group import Group


async def initialise_groups(account):
    groups = [Group(g, account) for g in await ret.get_groups(account)]

    log.info(f'Found {len(groups)} groups to scan through.')
    groups = fh.update_groups(groups)

    return groups


async def initialise_crawlers(account_strings, week_datetime, is_present):
    groups = chain.from_iterable([await initialise_groups(acc) for acc in account_strings])
    groups = list(set(filter(lambda g: g.active, groups)))
    time.sleep(30)
    language_strings = set(map(lambda g: g.language, groups))
    languages = {}
    for s in language_strings:
        languages[s] = Crawler(s, week_datetime, is_present=is_present)
    for g in groups:
        languages[g.language].add_group(g)

    return languages.values()


async def login_accounts():
    api_values = fh.get_api_values()

    if api_values is None:
        return None

    for label, values in api_values.items():
        await ret.login(label, fh.get_session(label), values['api_id'], values['api_hash'])

    return api_values.keys()


async def main(week_arg, languages, woi=False, log_heap=False):
    if woi:
        log.info('The frequencies of words are being recalculated. This can take a few minutes.')
        words_of_interests = fh.get_all_words_of_interest()
        languages = words_of_interests.keys()
        word_frequencies = fh.get_all_word_frequencies(languages)

        for lang in languages:
            for freq, path in tqdm(word_frequencies[lang], desc=f'Updating frequencies for {lang}'):
                filtered_freq = proc.filter_single_word_frequencies(freq, words_of_interests[lang])
                fh.write_csv(filtered_freq, path)

        return

    week = ret.get_week(week_arg)

    labels = await login_accounts()

    if labels is None:
        log.error(
            f"Either there was an issue with the formatting of the given api_values or there were none given. Exiting the application..")

        return

    time.sleep(30)
    crawlers = await initialise_crawlers(labels, week, week_arg is None)

    if languages is not None:
        log.info('Setup is running..')

        fh.create_input_directories(languages)
        log.info('Setup complete!')

        return

    else:
        for crawler in crawlers:
            await crawler.run(log_heap)

    for acc in labels:
        await ret.disconnect(acc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--setup', nargs='*',
                        help='<Optional> Insert a list of languages to setup the application.\n\tExample: --setup english german ukrainian',
                        default=None)
    parser.add_argument('--week',
                        help="<Optional> A date string in the form '%%Y-%%m-%%d' to specify the week to analyze.\n\tExample: --week 2022-02-14",
                        type=str, default=None)
    parser.add_argument('--woi',
                        help="<Optional> A flag to recalculate all 'Words_of_Interest-Frequencies.csv'.\n\tUse this if you changed the input file(s) 'words-of-interest.csv' and want to change the data retrospectively.\n\tExample: --woi",
                        action='store_true')
    parser.add_argument('--log-heap', action='store_true',
                        help='<Optional> Use to log heap memory allocation during the analysis process.\n\tExample: --log-heap')
    args = parser.parse_args()

    fh.setup_log()
    try:
        asyncio.run(main(args.week, args.setup, args.woi, args.log_heap))
    except Exception as e:
        log.error('The Research-App encountered an unexpected issue and stopped execution.')
        log.error(f'\n+++++++++++++++++++\n{traceback.format_exc()}\n+++++++++++++++++++')
