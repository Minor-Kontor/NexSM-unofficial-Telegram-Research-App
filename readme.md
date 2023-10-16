# NexSM-unofficial-Telegram-Research-App

NexSM-unofficial-Telegram-Research-App is a small Python application for the retrieval and processing of public messages in Telegram chats. The application utilizes the [Telethon library](https://docs.telethon.dev/en/stable/) which is based on the official [Telegram-API](https://core.telegram.org/).

## How to install

1. Download this repository, either with `git clone` or by download and unzipping.
2. Install Python. The application has been implemented and tested under Python 3.9.15.
3. Install pip: `python -m ensurepip --upgrade`.
4. Go into the root of the project.
5. Install required libraries: `pip install -r requirements.txt`

### How to update

To update the application to the newest version you can run `git pull` inside the project.
If you didn't use `git` to download the application you can download and unzip the project.

## How to use

Before the first use of the application some input files need to be modified and the application needs to set up.

### Telegram API values

The file `./inputs/api_values.csv` needs to be modified to run the application. It contains 3 columns: `label, api_id, api_hash`. The field `label` is optional, can be chosen freely and is used to distinguish between multiple accounts. The fields `api_id` and `api_hash` are unique per Telegram-Account and can be created and managed on https://www.my.telegram.org.  
If you want to use the application for multiple accounts you need to add a line for each account and create `api_id` and `api_hash` for each.

### Application setup

After you have modified the `api_values.csv` the application is almost ready to run for the first time. To run the setup of the application you need to run `python main.py --setup [LANGUAGES]`. You should specify language groups that you wish to analyze seperated by whitespaces. E.g.: `python main.py --setup german english ukrainian persian`.  
This will scan through all accounts mentioned in the `api_values.csv` and create additional input files for all given languages.

When the application is executed for the first time the Telegram-API will ask for the phone numbers related to the accounts and send a login code to the related account.
When the application is logged out for any reason the login process can be started exclusively with the flag `--login`.

### Additional inputs

The `inputs` folder contains input files for each language and input files that apply to all languages.

#### File `./inputs/api_values.csv`

This file was already discussed above. It contains all necessary information for the used Telegram accounts.

#### File `./inputs/groups.csv`

This file contains all groups that the accounts are in. This `.csv` is populated and updated whenever the application is executed. You can adjust the values in the `language` column to specify the language that is written in this group. All groups belonging to the same language will be processed together. If the field is left empty the language is assumed to be `unknown`. All groups in unknown will be processed together.

#### File `./inputs/first_names.json`

The application tries to approximate gender distribution in groups by comparing first names of participants to the list of names and associated gender in this `.json`. You can extend this `.json` with additional names to further improve this approximation.

#### File `./inputs/words-of-interest.csv`

This file contains all words that you want to look for in the application's analysis. This is a global file that is applied to all languages. For each language you specified an additional local file is created (e.g. `./inputs/german/words-of-interest.csv`).  
The column `translation` is optional for your own overview.

#### File `./inputs/words-to-ignore.csv`

This file contains all words that should be ignored during the application's analysis. This is a global file that is applied to all languages. For each language you specified an additional local file is created (e.g. `./inputs/german/words-to-ignore.csv`).  
This file should be filled with transitional/linking words to improve the output of the application. Examples in English could be: 'and', 'in', 'at', ...

### How to execute

Once you ran the setup and adjusted all the input files to your liking you can execute the application. To execute you can run the following command `python main.py` in the project root. This will retrieve and process data from the past full week from Monday to Sunday. Additionally, it will retrieve and process current participant data.

#### `--week`

If you wish to analyze messages from a specific week you can run the application with the optional `--week` argument. If you were interested in data surrounding Valentine's Day 2022 you could run `python main.py --week 2022-02-14`. This would result in the analysis running from the `2022-02-14` to `2022-02-20`.

The application will always run from Monday to Sunday regardless which weekday you give as an argument.

Keep in mind that due to restrictions and protection against spammers it is necessary to delay in between requests. Therefore, the retrieval and analysis of each group takes about 70 seconds. Depending on your use-case it might make sense to run the application scheduled with something like [crontab](https://man7.org/linux/man-pages/man5/crontab.5.html).

#### `--woi`

If you wish to recalculate data based on updated words-of-interests, you can update the relevant `words-of-interest.csv`-files and then run the Research-App with the flag `--woi`. This will take several minutes depending on the size of the already created data and will affect *all* data.

It recreates all the `Words_of_Interest-Frequencies.csv`-files based on the `Word-Frequencies.csv`-files.

### Outputs

All outputs can be found in the `./outputs` folder. The outputs are subdivided in languages. For each language there exists a `./outputs/[LANGUAGE]/Total-Overview.csv` containing participants numbers and estimation of gender per language. Additionally, a folder is created for each execution of the application named after the Monday of the relevant week (e.g. `2022-02-14`).

There is a folder for each group analysed called `group_[GROUP_ID]` and combined analysis of all groups of a language for each week.

- `Group-Overview`: Contains an overview of participants and number of messages in that week.
- `Daily-Activity`: Activity by day as a `.png` and as a `.csv`.
- `Hourly-Activity`: Activity by hour as a `.png` and as a `.csv`.
- `Weekly-Joins`: People joining a group of the language for the week as a `.png`.
- `Word-Frequencies.csv`: List of used words and their frequencies over the week.
- `Words_of_Interest-Frequencies.csv`: List of the number of occurrences of words of interest.
- `Words_of_Interest-Pair-Frequencies.csv`: List of pairs of words containing at least one word of interest and their frequency.
- `Group-Participant-Network.csv`: List of anonymized participants and in which groups they are for further network analysis.