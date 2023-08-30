
from pypdf import PdfReader
from pdfminer.high_level import extract_text #pdfminer.six
import tabula #tabula-py
import yaml  # PyYAML
import math

import os
import sys
import re
from decimal import Decimal
import csv
from datetime import date, datetime, timedelta
import time

start_time = time.time()

with open(r"C:\Users\jazzo\Desktop\academics\internships\internad\Sharpinvest\python_test\pyDataCapture\SI_scrape_config.yaml", 'r', encoding='utf-8') as conf_f:
    config = yaml.safe_load(conf_f)
    # print(config)
    # print(yaml.dump(config))

csv_gen = config['export_csv']


# bool to indicate whether to export scraped data into a csv or not.


def last_day_of_month(dt):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = dt.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return (next_month - timedelta(days=next_month.day)).day


# set raw_report_date int to choose input batch (month); 202205 for May 2022, 202211 for Nov 2022 etc.
# raw_report_date = args.month
raw_report_date = config['report_month']
report_year = int(str(raw_report_date)[:4])
report_month = int(str(raw_report_date)[4:])
report_date = date(report_year, report_month, last_day_of_month(date(report_year, report_month, 1)))
# year, month and date of batch of reports. Also part of filepath (name of folder containing the pdfs)


csv_out = []
# initialize csv file
tdy = date.today()
now = datetime.now()

tdystr = tdy.strftime('%Y%m%d')
nowstr = now.strftime('%Y%m%d%H%M%S')

csv_name = config['output_csv_fp'] + 'output_' + str(raw_report_date) + '_' + nowstr + '.csv'
# rename nowstr to tdystr if intended to run daily

# print(csv_name)
filenames = []
try:
    with open(csv_name, "r", encoding='utf_16', newline='') as csv_read:
        reader = csv.reader(csv_read, delimiter=",")
        filenames = []
        for row in reader:
            filenames.append(row[0].split(',')[0])
        # print(filenames)
except FileNotFoundError:
    pass

logger_fp = config['logger_fp'] + 'output' + '_' + nowstr + '.txt'


class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(logger_fp, 'w', encoding='utf_16')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # python 3 compatibility
        pass

    def close(self):
        self.log.close()


if config['export_logger']:
    sys.stdout = Logger()


# Logger class exported terminal output to a txt file. used for debugging


def readpdf(filepath):  # pypdf
    filename = os.path.basename(filepath)
    try:
        parsed = PdfReader(filepath)
    except:
        return ''

    # pdf_metadata = parsed.metadata

    first_page_text = parsed.pages[0].extract_text()

    all_pages_text = first_page_text

    number_of_pages = len(parsed.pages)
    if number_of_pages > 1:
        for page_no in range(1, number_of_pages):
            page = parsed.pages[page_no]
            page_content = page.extract_text()
            all_pages_text += '\n' + page_content

    if config['export_scanned_text']:
        txt_name = config['scraped_txt_fp'] + str(
            raw_report_date) + '/' + filename.replace('.pdf', '') + '_raw.txt'
        try:
            with open(txt_name, 'w', encoding='utf_16') as f:
                pass
        except FileNotFoundError:
            os.mkdir(config['scraped_txt_fp'] + str(raw_report_date))
            with open(txt_name, 'x', encoding='utf_16') as f:
                pass

        with open(txt_name, 'w', encoding='utf_16') as f:
            f.write(all_pages_text)
    # '''
    # ^ created txt file of pdf content if needed, so that i can view the raw scraped text for debugging
    return all_pages_text


def minepdf(filepath):  # pdfminer
    filename = os.path.basename(filepath)
    try:
        mined = extract_text(filepath)
    except:
        return ''

    # print(mined)
    return mined


def tabupdf(filepath):
    try:
        dfs = tabula.read_pdf(filepath, pages='all', guess=False, stream=True)
        if config['export_scanned_tabu']:
            tab_txt_name = config['scraped_txt_fp'] + str(
                raw_report_date) + '/' + os.path.basename(filepath).replace('.pdf', '') + '_tabDataframe.csv'
            try:
                with open(tab_txt_name, 'w', encoding='utf_16') as f:
                    pass
            except FileNotFoundError:
                os.mkdir(config['scraped_txt_fp'] + str(raw_report_date))
                with open(tab_txt_name, 'x', encoding='utf_16') as f:
                    pass

            with open(tab_txt_name, 'w', encoding='utf_16') as f:
                for n, df in enumerate(dfs):
                    csv.writer(f).writerow(['Table ' + str(n)])
                    df.to_csv(f)
        return dfs
    except Exception as e:
        # print(e)
        return e


twomonthsago = report_date - timedelta(days=63)


# Jul and Aug both have 31 days and are consecutive. 31+31+1=63

def filterdates(date_list):
    date_list_len_history = []
    date_list_len_history.append(len(date_list))

    date_list = list(dict.fromkeys(date_list))
    # remove duplicates from list

    date_list_len_history.append(len(date_list))

    dt_to_remove = []

    date_list = [d for d in date_list if not (d.date() > report_date or d.date() < twomonthsago)]

    dt_to_remove = []

    date_list_len_history.append(len(date_list))

    edge_days = []
    # 28, 29, 30, 31? edit if necessary

    for dt in date_list:
        if dt.day > 5:
            # if dt.month != (dt + timedelta(days=2)).month:
            edge_days.append(dt)

    date_list_len_history.append(len(edge_days))

    # print('date list len history:', date_list_len_history)
    return (edge_days)


def find_word(content, strlist, badstrlist, output):
    lines_w_digits = []
    lines_raw = []
    lines = content
    for line in lines:
        n_line = ''.join(c if c.isdigit() else c if c == ' ' else ' ' for c in line)
        n_line = ' '.join(n_line.split())
        if n_line != '' and n_line != '':
            for string in strlist:
                # check if string present on a current line
                str_index = line.find(string)
                if str_index != -1:
                    nobadstr = True
                    for badstring in badstrlist:
                        if line.find(badstring) != -1:
                            nobadstr = False
                            break

                    if nobadstr:
                        lines_w_digits.append(n_line)
                        lines_raw.append(line[str_index:])
                        break

    if output == 'n':  # numerical only (for dates)
        return lines_w_digits
    elif output == 'r':  # raw text
        return lines_raw
    else:
        raise SyntaxError('output of find_word')


def find_word_all(content, strlist, badstrlist):
    lines_raw = []
    lines = content
    for line in lines:
        for string in strlist:
            # check if string present on a current line
            str_index = line.find(string)
            if str_index != -1:
                nobadstr = True
                for badstring in badstrlist:
                    if line.find(badstring) != -1:
                        nobadstr = False
                        break
                if nobadstr:
                    lines_raw.append(line[str_index:])
                    break
    return lines_raw


def tabula_kw_find(data, keywords):
    lines = []
    for i, line in enumerate(data):
        for kw in keywords:
            for c, cell in enumerate(line):
                if type(cell) == str:
                    str_index = cell.find(kw)
                    if str_index != -1:
                        lines.append(line[c:])
                        break
    return lines


def trim(lines):
    # lines_temp = [l for l in lines if len(l) > 1]
    lines_temp = lines[:]

    if len(lines_temp) == 1:
        return lines_temp[0]
    elif len(lines_temp) == 2 and lines[0] == lines[1]:
        return lines_temp[0]
    elif len(lines_temp) == 3 and lines[0] == lines[1] == lines[2]:
        return lines_temp[0]
    else:
        lines_temp_comb = []
        for l in lines_temp:
            lines_temp_comb += l
        lines_temp_comb_nodupe = list(dict.fromkeys(lines_temp_comb))
        return lines_temp_comb_nodupe


total_files = 0
extracted_files_date = 0
correct_files_date = 0
extracted_files_aum = 0
manager_found_files = 0
bank_found_files = 0
shares_extracted_files = 0

dir_path = config['report_pdf_fp']  # a.encode('unicode_escape')
dir_path_unread = dir_path + str(raw_report_date)

dir_path_txt = config['scraped_txt_fp'] + str(raw_report_date)
ext = 'pdf'

curr_year = int(date.today().year)
recent_years = [str(curr_year), str(curr_year - 1), str(curr_year - 1911), str(curr_year - 1911 - 1)]

recent_years_raw = [str(curr_year), str(curr_year - 1)]
for year in recent_years_raw:
    recent_years.append(year[:2] + ' ' + year[2:])
    # fix cases where 20 22, 20 19 is scraped instead of 2022, 2019


test_list = ['Allianz_NTDMD0110.pdf','Allianz_USDMD0100.pdf','Allianz_USDMD0190.pdf','Allianz_USDMD0260.pdf'
             'Allianz_USDMD0300.pdf','Allianz_USDMD0300X.pdf','CNTrustLife_A004.pdf','CNTrustLife_A005.pdf'
             'CNTrustLife_A006.pdf','CNTrustLife_A007.pdf','CNTrustLife_A008.pdf','CNTrustLife_M004.pdf'
             'Fubon_MLD1.pdf','MLI_MHY00010.pdf','MLI_MHY00011.pdf','Transglobe_SIC01.pdf','taishinlife_FF006.pdf']

for file in sorted(os.listdir(dir_path_unread)):  # + '//unread//'
    try:
        if file not in test_list:
            #continue#
            pass

        if '.pdf' not in file:
            continue  # only check pdfs

        if file not in filenames:
            total_files += 1
            fp = dir_path_unread + '//' + file  # filepath
            print('\n', total_files, file)

            if os.stat(fp).st_size == 0:
                raise Exception('File is empty!')

            dfs = tabupdf(fp)
            if str(type(dfs)) == '<class \'subprocess.CalledProcessError\'>':  # catch corrupted files
                raise Exception('tabula error when accessing file. Check if PDF is corrupted')

            pdf_content_all = []
            pdf_content_all = minepdf(fp)
            pdf_content_all = pdf_content_all.split('\n')

            # <editor-fold desc='AUM'>
            # AUM COLLECTION START
            aum_keywords = config['aum_keywords']
            aum_keywords_secondary = config['aum_keywords_1']
            aum_keywords_avoid = config['aum_avoid']  # 費規模 because 經理費規模, 保管費規模

            report_output_AUM = 'AUM NF'
            aum_lines = []
            # tabu start here
            for table in dfs:

                # for tabu:
                data_arr = table.to_numpy().tolist()
                # data_arr = table.df.to_numpy().tolist()
                # print(data_arr)

                for i, line in enumerate(data_arr):
                    for kw in config['aum_keywords']:
                        for c, cell in enumerate(line):
                            if type(cell) == str:
                                str_index_aum = cell.find(kw)
                                if str_index_aum != -1:
                                    aum_lines.append(line[c:])
                                    break

            for i, l in enumerate(aum_lines):
                aum_lines[i] = [x for x in l if type(x) == str]
                temp_aum_line = []
                for j, x in enumerate(aum_lines[i]):
                    temp_aum_line += x.split(' ')
                aum_lines[i] = temp_aum_line

            aum_lines = [l for l in aum_lines if not (('規' in l) and ('模' in l) and ('0' in l))]

            aum_lines = trim(aum_lines)

            #print(aum_lines)

            aum_avoid = list('%')
            # aum_lines = [s for s in mng_lines if not any([dig in s for dig in digits])]

            if len(aum_lines) > 0:
                for i, l in enumerate(aum_lines[:]):
                    if aum_lines[i] in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
                        aum_lines[i] += '.0'

            aum_lines = [s for s in aum_lines if len(s) > 1]

            aum_lines = [s.replace('百萬', '百萬 ').split(' ') for s in aum_lines]
            aum_lines = sum(aum_lines, [])

            aum_lines_prebrack = aum_lines

            aum_lines = [s.replace('百萬', '百萬 ').split(' ') for s in aum_lines]
            aum_lines = sum(aum_lines, [])
            aum_lines = [s.replace('（百萬', ' 百萬').replace('(百萬', ' 百萬').split(' ') for s in aum_lines]
            aum_lines = sum(aum_lines, [])
            aum_lines = [s.replace('MSCI', '') for s in aum_lines]

            #print('replace', aum_lines)

            words_to_keep_postcurr_list = list('0123456789%.百佰千仟萬億兆MBK') + ['mil', 'bil']
            chars_to_keep_postcurr_list = list('0123456789%.百佰千仟萬億兆MBK') + list('mil' + 'bil')

            chars_avoid = list('%年月日-<≤>') + ['每單位提解']
            for i in range(7):
                chars_avoid += [str(int(str(raw_report_date)[:4]) - i) + '/']
                chars_avoid += ['/' + str(int(str(raw_report_date)[:4]) - i)]

            aum_lines_filt = []
            for i, s in enumerate(aum_lines):
                if any([char in s for char in words_to_keep_postcurr_list]):
                    if not any([char in s for char in chars_avoid]):
                        aum_lines_filt.append(s)

            aum_lines_filt_char = []

            for i, s in enumerate(aum_lines_filt[:]):
                aum_lines_filt[i] = s.replace('(', '').replace(')', '').replace('）', '').replace('（', '')

            for i, s in enumerate(aum_lines_filt):
                aum_lines_filt_char.append('')
                for j, char in enumerate(s):
                    if char in chars_to_keep_postcurr_list:
                        aum_lines_filt_char[i] += char

            #print('pre filt', aum_lines_filt_char)

            aum_lines_filt_char = list(dict.fromkeys(aum_lines_filt_char))

            aum_lines_filt_char = [s for s in aum_lines_filt_char if
                                   len(''.join(char for char in s if char == '.')) <= 1]
            aum_lines_filt_char = [s for s in aum_lines_filt_char if s[-1] != '.']

            if len(aum_lines_filt_char) >= 3:
                aum_lines_filt_char_str = ''.join(aum_lines_filt_char)

                if any(i in ['百', '佰', '千', '仟', '萬', '億', '兆'] for i in aum_lines_filt_char_str):
                    try:
                        float(aum_lines_filt_char[-1])
                        aum_lines_filt_char = aum_lines_filt_char[:-1]
                    except ValueError:
                        pass

                elif aum_lines_filt_char_str.replace('.', '').isdigit():
                    for i, s in enumerate(aum_lines_filt_char[:]):
                        try:
                            if float(s) < 5000:
                                aum_lines_filt_char.remove(s)
                        except ValueError:
                            pass

            aum_lines_filt_char = [l for l in aum_lines_filt_char if len(l.split('.')) <= 2]

            #print('post filt', aum_lines_filt_char)

            if len(aum_lines_filt_char) == 1:
                multiplier = 1
                if len(aum_lines_filt_char[0]) > 2:
                    if all([char in list('.0123456789') for char in aum_lines_filt_char[0]]) and Decimal(
                            aum_lines_filt_char[0]) > 5000:
                        report_output_AUM = Decimal(aum_lines_filt_char[0])


            elif len(aum_lines_filt_char) == 2:
                multiplier = 1
                for i, s in enumerate(aum_lines_filt_char):
                    if s == '億':
                        multiplier = 100000000
                    elif s == '千萬':
                        multiplier = 10000000
                    elif s == '百萬' or s == '佰萬':
                        multiplier = 1000000
                    elif s == '十萬' or s == '拾萬':
                        multiplier = 100000
                    elif s == '萬':
                        multiplier = 10000
                    elif s == '千' or s == '仟':
                        multiplier = 1000
                    elif s == '百' or s == '佰':
                        multiplier = 100
                    elif s == '十' or s == '拾':
                        multiplier = 10
                    elif s == 'M':
                        multiplier = 1000000
                    elif s == 'Mil' or s == 'mil' or s == 'MIL':
                        multiplier = 1000000
                    elif s == 'B':
                        multiplier = 1000000000
                    elif s == 'Bil' or s == 'bil' or s == 'BIL':
                        multiplier = 1000000000
                    elif s == 'k' or s == 'K':
                        multiplier = 1000
                    if len(''.join(char for char in aum_lines_filt_char[i - 1] if char == '.')) <= 1 and len(
                            aum_lines_filt_char[i - 1]) > 0:
                        if all([char in list('.0123456789') for char in aum_lines_filt_char[i - 1]]):
                            if Decimal(aum_lines_filt_char[i - 1]) * multiplier > 70000:
                                report_output_AUM = Decimal(aum_lines_filt_char[i - 1]) * multiplier
                                break

            if report_output_AUM == 'AUM NF':
                report_output_aum_multi = []
                for i, s in enumerate(aum_lines_filt_char[:]):
                    if len(s) > 3:
                        if not aum_lines_filt_char[i][-1].isdigit() and not aum_lines_filt_char[i][
                            -2].isdigit() and not \
                                aum_lines_filt_char[i][-3].isdigit() and all(
                            [char in list('.0123456789') for char in aum_lines_filt_char[i][:-3]]):
                            if aum_lines_filt_char[i][-3:] == 'Mil' or aum_lines_filt_char[i][-3:] == 'mil' or \
                                    aum_lines_filt_char[i][
                                    -3:] == 'MIL':
                                multiplier = 1000000
                            elif aum_lines_filt_char[i][-3:] == 'Bil' or aum_lines_filt_char[i][-3:] == 'bil' or \
                                    aum_lines_filt_char[i][
                                    -3:] == 'BIL':
                                multiplier = 1000000000
                            if multiplier != 1:
                                report_output_aum_multi.append(
                                    Decimal(aum_lines_filt_char[i][:-3]) * multiplier)

                        elif not aum_lines_filt_char[i][-1].isdigit() and not aum_lines_filt_char[i][
                            -2].isdigit() and all(
                            [char in list('.0123456789') for char in aum_lines_filt_char[i][:-2]]):
                            if aum_lines_filt_char[i][-2:] == '千萬':
                                multiplier = 10000000
                            elif aum_lines_filt_char[i][-2:] == '百萬' or aum_lines_filt_char[i][-2:] == '佰萬':
                                multiplier = 1000000
                            elif aum_lines_filt_char[i][-2:] == '十萬' or aum_lines_filt_char[i][-2:] == '拾萬':
                                multiplier = 100000
                            if multiplier != 1:
                                report_output_aum_multi.append(
                                    Decimal(aum_lines_filt_char[i][:-2]) * multiplier)

                        elif not aum_lines_filt_char[i][-1].isdigit() and all(
                                [char in list('.0123456789') for char in aum_lines_filt_char[i][:-1]]):
                            if aum_lines_filt_char[i][-1] == '億':
                                multiplier = 100000000
                            elif aum_lines_filt_char[i][-1] == '萬':
                                multiplier = 10000
                            elif aum_lines_filt_char[i][-1] == '千' or aum_lines_filt_char[i][-1] == '仟':
                                multiplier = 1000
                            elif aum_lines_filt_char[i][-1] == '百' or aum_lines_filt_char[i][-1] == '佰':
                                multiplier = 100
                            elif aum_lines_filt_char[i][-1] == '十' or aum_lines_filt_char[i][-1] == '拾':
                                multiplier = 10
                            elif aum_lines_filt_char[i][-1] == 'M':
                                multiplier = 1000000
                            elif aum_lines_filt_char[i][-1] == 'B':
                                multiplier = 1000000000
                            elif aum_lines_filt_char[i][-1] == 'k' or aum_lines_filt_char[i][-1] == 'K':
                                multiplier = 1000
                            if multiplier != 1:
                                report_output_aum_multi.append(
                                    Decimal(aum_lines_filt_char[i][:-1]) * multiplier)

                for aum in report_output_aum_multi:
                    if aum > 5000:
                        report_output_AUM = aum

            if type(report_output_AUM) == float:
                report_output_AUM = round(report_output_AUM, 2)

            print('AUM:', report_output_AUM)

            if report_output_AUM == 'AUM NF':
                if config['move_pdfs']:
                    os.renames(fp, dir_path + 'MonthlyRpt_cannotread//' + str(raw_report_date) + '//' + file)

            else:
                extracted_files_aum += 1
                if config['move_pdfs']:
                    os.renames(fp, dir_path + 'MonthlyRpt_read//' + str(raw_report_date) + '//' + file)
                pass

            # AUM COLLECTION END
            # </editor-fold>

            # <editor-fold desc='Date'>

            # DATE COLLECTION START

            report_output_date = 'Date NF'

            date_status = ''
            # date_keywords = ['截至', '截止日期', '報告日期', '資料日期']
            # date_keywords = ['月', '年', '/', '報告']
            # date_keywords_avoid = ['樓', '電話'] # '成立',
            date_keywords_avoid = []

            pdf_content = []

            pdf_content = readpdf(fp)
            # pdf_content is str
            pdf_content = pdf_content.replace('20 ', '20')
            pdf_content = pdf_content.replace(str(report_year)[:3] + ' ' + str(report_year)[3:], str(report_year))
            pdf_content = pdf_content.replace(str(report_year)[:2] + ' ' + str(report_year)[2:], str(report_year))
            pdf_content = pdf_content.replace(str(report_year)[:1] + ' ' + str(report_year)[1:], str(report_year))
            pdf_content = pdf_content.replace(' ', '')
            pdf_content = pdf_content.replace(str(report_year), ' ' + str(report_year))
            pdf_content = pdf_content.split('\n')

            digit_lines = find_word(pdf_content, config['date_keywords'], date_keywords_avoid, 'n')

            if len(digit_lines) == 0:
                digit_lines = find_word(pdf_content, config['date_keywords_1'], date_keywords_avoid, 'n')

            if len(digit_lines) == 0:
                digit_lines = find_word(pdf_content, config['date_keywords_2'], date_keywords_avoid, 'n')

            digit_lines_conc = []

            for l in digit_lines:
                for year in recent_years:
                    year_pos = l.find(str(year))

                    if year_pos != -1:
                        digit_lines_conc.append(l[year_pos:])

            for i, l in enumerate(digit_lines):
                if len(l) in [4, 5] and l[0] == '2':
                    for year in recent_years:
                        if l == year and i != len(digit_lines) - 1 and len(digit_lines[i + 1]) <= 2:
                            if 0 < int(digit_lines[i + 1]) <= 12:
                                digit_lines_conc.insert(0, digit_lines[i] + digit_lines[i + 1])
                                break

            nested_digit_lines = []
            for l in digit_lines_conc:
                if len(l) > 10:
                    for year in recent_years:
                        nested_year_pos = l[1:].find(str(year))
                        if nested_year_pos != -1:
                            nested_digit_lines.append(l[1:][nested_year_pos:])

            digit_lines_conc += nested_digit_lines
            # print('digit lines conc:', digit_lines_conc)
            digit_lines_conc_filt = []

            for i, l in enumerate(digit_lines_conc[:]):
                if l[2] == ' ':
                    digit_lines_conc[i] = l.replace(' ', '', 1)

            for l in digit_lines_conc:
                if len(l) >= 5:
                    l += '   '
                    if l[4] == ' ':
                        if l[5] == '0' and l[6] in ['1', '2', '3', '4', '5', '6', '7', '8', '9'] and l[7] == ' ':
                            if l[8] == ' ':
                                digit_lines_conc_filt.append(l[:7])
                            elif 0 < int(l[8:10]) <= 31:
                                digit_lines_conc_filt.append(l[:10])

                        elif l[5] == '1' and l[6] in ['0', '1', '2'] and l[7] == ' ':
                            if l[8] == ' ':
                                digit_lines_conc_filt.append(l[:7])
                            elif 0 < int(l[8:10]) <= 31:
                                digit_lines_conc_filt.append(l[:10])

                        elif l[5] in ['1', '2', '3', '4', '5', '6', '7', '8', '9'] and l[6] == ' ':
                            if l[7] == ' ':
                                digit_lines_conc_filt.append(l[:6])
                            elif 0 < int(l[7:9]) <= 31:
                                digit_lines_conc_filt.append(l[:9])

                    elif l[3] != ' ':
                        if l[4] == '0' and l[5] in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
                            if l[6] == ' ':
                                digit_lines_conc_filt.append(l[:6])
                            elif 0 < int(l[6:8]) <= 31:
                                digit_lines_conc_filt.append(l[:8])


                        elif l[4] == '1' and l[5] in ['0', '1', '2']:
                            if l[6] == ' ':
                                digit_lines_conc_filt.append(l[:6])
                            elif 0 < int(l[6:8]) <= 31:
                                digit_lines_conc_filt.append(l[:8])

                        elif l[4] in ['1', '2', '3', '4', '5', '6', '7', '8', '9'] and len(l) == 5:
                            digit_lines_conc_filt.append(l)

                        elif l[4] in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
                            if l[5] == ' ':
                                digit_lines_conc_filt.append(l[:5])
                            elif 0 < int(l[5:7]) <= 31:
                                digit_lines_conc_filt.append(l[:7])

            # 民國years
            if digit_lines_conc_filt == []:
                for l in digit_lines_conc[:]:
                    if len(l) >= 5:
                        l += '   '
                        if l[3] == ' ':
                            if l[4] == '0':
                                if l[5] in ['1', '2', '3', '4', '5', '6', '7', '8', '9'] and l[6] == ' ':
                                    if l[7] == ' ':
                                        digit_lines_conc_filt.append(l[:6])
                                    elif 0 < int(l[7:9]) <= 31:
                                        digit_lines_conc_filt.append(l[:9])
                                elif l[5] == ' ':
                                    ltemp = l[:5] + l[6:]
                                    # print(ltemp)
                                    digit_lines_conc.append(ltemp)


                            elif l[4] == '1' and l[5] in ['0', '1', '2'] and l[6] == ' ':
                                if l[7] == ' ':
                                    digit_lines_conc_filt.append(l[:6])
                                elif 0 < int(l[7:9]) <= 31:
                                    digit_lines_conc_filt.append(l[:9])

                            elif l[4] in ['1', '2', '3', '4', '5', '6', '7', '8', '9'] and l[5] == ' ':
                                if l[6] == ' ':
                                    digit_lines_conc_filt.append(l[:5])
                                elif 0 < int(l[6:8]) <= 31:
                                    digit_lines_conc_filt.append(l[:8])

            # print('digit lines conc filt:', digit_lines_conc_filt)

            if digit_lines_conc_filt == []:
                report_output_date = 'Date NF'
            else:
                date_ym_dt = []

                for dt in digit_lines_conc_filt:
                    date_split = dt.split(' ')
                    if len(date_split) == 3:
                        if 1 <= int(date_split[1]) <= 12:
                            if 1 <= int(date_split[1]) <= 31:
                                try:
                                    date_ym_dt.append(date(int(date_split[0]), int(date_split[1]), int(date_split[2])))
                                except ValueError:
                                    date_ym_dt.append(
                                        date(int(date_split[0]), int(date_split[1]), int(date_split[2]) - 1))

                            else:
                                date_ym_dt.append(date(int(date_split[0]), int(date_split[1]),
                                                       last_day_of_month(
                                                           date(int(date_split[0]), int(date_split[1]), 1))))
                    elif len(date_split) == 2 and date_split[1] != '':
                        if 1 <= int(date_split[1]) <= 12:
                            date_ym_dt.append(date(int(date_split[0]), int(date_split[1]),
                                                   last_day_of_month(date(int(date_split[0]), int(date_split[1]), 1))))

                for i, dt in enumerate(date_ym_dt[:]):
                    if dt.year in [report_year - 1910, report_year - 1911, report_year - 1912]:
                        date_ym_dt[i] = date_ym_dt[i].replace(year=dt.year + 1911)

                dt_to_remove = []
                for dt in date_ym_dt:
                    if dt > report_date or dt < twomonthsago:
                        dt_to_remove.append(dt)

                date_ym_dt = [d for d in date_ym_dt if d not in dt_to_remove]
                # for dt in dt_to_remove:
                #    date_ym_dt.remo ve(dt)

                if date_ym_dt == []:
                    report_output_date = 'Date NF'
                else:
                    report_output_date = sorted(date_ym_dt, reverse=True)[0]

            if type(report_output_date) is not str:
                extracted_files_date += 1

            elif report_output_date == 'Date NF':
                date_status += '[date not found] '

            # to test accuracy of program. To be #ed out

            if raw_report_date == 202205:
                if report_output_date == date(2022, 5, 31):
                    correct_files_date += 1
            elif raw_report_date == 202204:
                if report_output_date in [date(2022, 4, 29), date(2022, 4, 30)]:
                    correct_files_date += 1
            elif raw_report_date == 202207:
                if report_output_date in [date(2022, 7, 31), date(2022, 7, 30), date(2022, 7, 29)]:
                    correct_files_date += 1

            # DATE COLLECTION END
            # </editor-fold>

            # <editor-fold desc='Name'>
            # NAME COLLECTION START

            # name_keywords = ['人壽', '帳戶']
            # name_keywords_avoid = []

            name_lines = []

            for line in pdf_content_all:
                if '人壽' in line:
                    for kw in config['prodname_keywords']:
                        if kw in line:
                            name_lines.append(line)
                            break

            name_lines_prereplace = name_lines + []

            name_lines_repl = []
            for i, line in enumerate(name_lines[:]):
                replace_chars = list('，。、.%/樓：*') + config['prodname_avoid'] + [file[:len(file) - 4]]
                for c in replace_chars:
                    name_lines[i] = name_lines[i].replace(c, ';')
                # name_lines[i] = name_lines[i].replace('帳戶', '帳戶;')
                name_lines[i] = name_lines[i].replace('僅限', ';僅限;')
                # name_lines[i] = name_lines[i].replace('須依', ';須依;')
                name_lines[i] = name_lines[i].replace(')(', ');(')
                name_lines[i] = name_lines[i].replace(') (', ');(')
                name_lines[i] = name_lines[i].replace('） （', ');(')
                name_lines[i] = name_lines[i].replace('）（', ');(')
                name_lines[i] = name_lines[i].replace(')', ');')
                name_lines[i] = name_lines[i].replace('  ', ' ;')

                name_lines_repl_el = []
                name_lines_repl_el += name_lines[i]
                name_lines_repl.append(name_lines_repl_el)

            for i, sen_sep in enumerate(name_lines_repl[:]):
                sen = ''
                for char in sen_sep:
                    sen += char
                name_lines_repl[i] = sen

            name_lines_split = []
            for i, line in enumerate(name_lines_repl):
                temp_line_split = line.split(';')
                for inner_line in temp_line_split:
                    if inner_line != '':
                        name_lines_split.append(inner_line)

            name_lines_filt = []
            for line in name_lines_split:
                if '人壽' in line and len(line) > 6:
                    for kw in config['prodname_keywords']:
                        if kw in line:
                            name_lines_filt.append(line)
                            break

            for i, line in enumerate(name_lines_filt[:]):
                start_index = 0
                for j, char in enumerate(line):
                    if char in list('0123456789 *-().,‧'):  # if these chars at start of str, remove them
                        start_index = j + 1
                    else:
                        break
                name_lines_filt[i] = name_lines_filt[i][start_index:]

            name_lines_filt = list(set(name_lines_filt))

            if len(set(name_lines_filt)) == 1 and len(name_lines_filt) > 1:  # removes duplicates
                name_lines_filt = [name_lines_filt[0]]

            report_output_prodname = []

            if name_lines_filt == []:
                name_lines_filt = []
            elif len(name_lines_filt) == 1:
                report_output_prodname = [name_lines_filt[0]]
            elif len(name_lines_filt) > 1:
                report_output_prodname = name_lines_filt
                name_lines_filt_ordered = sorted(name_lines_filt, key=len)

                report_output_prodname = name_lines_filt_ordered

                name_lines_filt_ordered = [n for n in name_lines_filt_ordered if n != '']

                # name_lines_filt_ordered = ' '.join(name_lines_filt_ordered.split())

                report_output_prodname = name_lines_filt_ordered

                if len(name_lines_filt_ordered) == 2:
                    if name_lines_filt_ordered[0] in name_lines_filt_ordered[1]:
                        report_output_prodname = [name_lines_filt_ordered[0]]

                    else:
                        bool_list_charin2 = []
                        for char in name_lines_filt_ordered[0]:
                            bool_list_charin2.append(char in name_lines_filt_ordered[1])
                        if all(bool_list_charin2):
                            report_output_prodname = [name_lines_filt_ordered[0]]

                        else:
                            report_output_prodname = ['']  # , '']
                            for char in name_lines_filt_ordered[0]:
                                if char in name_lines_filt_ordered[1]:
                                    report_output_prodname[0] += char


                elif len(name_lines_filt_ordered) > 2:
                    list_of_lines = name_lines_filt_ordered[1:]
                    bool_list_linesin = []
                    for l in list_of_lines:
                        bool_list_linesin.append(name_lines_filt_ordered[0] in l)
                    if all(bool_list_linesin):
                        report_output_prodname = [name_lines_filt_ordered[0]]

                    else:
                        bool_list_charin3 = []
                        for l in list_of_lines:
                            for char in name_lines_filt_ordered[0]:
                                bool_list_charin3.append(char in l)
                        if all(bool_list_charin3):
                            report_output_prodname = [name_lines_filt_ordered[0]]
                        else:
                            report_output_prodname = ['']  # , '', '']
                            for char in name_lines_filt_ordered[0]:
                                if char in name_lines_filt_ordered[1] and char in name_lines_filt_ordered[2]:
                                    # only valid for len3
                                    report_output_prodname[0] += char

            if report_output_prodname == []:
                report_output_prodname = 'ProdName NF'
            elif len(report_output_prodname) == 1:
                report_output_prodname = report_output_prodname[0]

            if len(report_output_prodname) <= 4:
                report_output_prodname = 'ProdName NF'

            # </editor-fold>

            # <editor-fold desc='Manager'>
            # 經理人（一定有）

            tw_lastnames_10 = list('陳林黃張李王吳劉蔡楊')
            tw_lastnames_50 = list('許鄭謝洪郭邱曾廖賴徐周葉蘇莊呂江何蕭羅高潘簡朱鍾游彭詹胡施沈余盧梁趙顏柯翁魏孫戴')
            tw_lastnames_200 = ['張簡', '歐陽'] + list('范方宋鄧杜傅侯曹薛丁卓阮馬董温唐藍石蔣古紀姚連馮歐程湯黄田康姜白汪鄒尤巫鐘'
                                                       '黎涂龔嚴韓袁金童陸夏柳凃邵錢伍倪溫于譚駱熊任甘秦顧毛章史官萬俞雷粘饒闕'
                                                       '凌崔尹孔辛武辜陶段龍韋葛池孟褚殷麥賀賈莫文管關向包丘梅')
            # 利裴樊房全佘左花魯安鮑郝穆塗邢蒲成谷常閻練盛鄔耿聶符申祝繆陽解曲岳齊籃應單舒畢喬龎翟牛鄞留季') + ['范姜']
            tw_lastnames_all = tw_lastnames_10 + tw_lastnames_50 + tw_lastnames_200

            names_ban = list('!\'#$%&\'()*+, 012346789abcdewfghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYUZ'
                            '-/<=>?@[]^_`{|}~,.;:()：；，。（）「」【】＋、')
            names_incor = ['成立', '簡介月', '人壽', '全委', '經理', '管費', '管理', '全權', '投信', '全球',
                           '單位', '位淨', '波動', '大學', '機構', '預期', '顧問', '不', '一個', '投顧', '製造',
                           '勝率', '本月', '人', '金融', '與','向下','連續']
            manager_lines = []
            manager_names = []
            report_output_manager = ''

            for table in dfs:
                data_arr = table.to_numpy().tolist()
                for i, line in enumerate(data_arr):
                    for kw in config['manager_keywords']:
                        for c, cell in enumerate(line):
                            if type(cell) == str:
                                str_index_mng = cell.find(kw)
                                if str_index_mng != -1:
                                    manager_lines.append(line[c:])
                                    break

            for i, l in enumerate(manager_lines):
                manager_lines[i] = [x for x in l if type(x) == str]
                temp_mng_line = []
                for j, x in enumerate(manager_lines[i]):
                    temp_mng_line += x.split(' ')
                manager_lines[i] = temp_mng_line

            manager_lines = trim(manager_lines)

            digits = list('0123456789%')

            manager_lines = [s for s in manager_lines if not any([c in s for c in digits+names_ban])]
            manager_lines = [s for s in manager_lines if len(s) > 1]
            #print(manager_lines)

            for name in manager_lines:
                if len(name) == 3 and name[0] in tw_lastnames_all:
                    report_output_manager = name
                    manager_found_files += 1
                    break
            if report_output_manager == '':
                report_output_manager = 'MngName NF'

            if report_output_manager == 'MngName NF':
                empty = True
                reloop = False
                while empty:
                    for i, line in enumerate(pdf_content_all):
                        for kw in config['manager_keywords']:
                            if kw in line:
                                mng_kw_index = line.find(kw)
                                if reloop:
                                    if len(pdf_content_all) > i + 1:  # TBD
                                        manager_lines.append(pdf_content_all[i + 1][:15])
                                        # manager_lines.append(pdf_content_all[i-1][:15])
                                        break
                                manager_lines.append(line[mng_kw_index:mng_kw_index + 15])
                                break
                    for line in manager_lines:
                        name_index = -1
                        for name in tw_lastnames_10:
                            if name in line:
                                name_index = line.find(name)
                                manager_names.append(line[name_index:name_index + 3])
                                # print('top 10 name found in:', line)
                        if line[name_index:name_index + 3] not in manager_names:
                            for name in tw_lastnames_50:
                                if name in line:
                                    name_index = line.find(name)
                                    manager_names.append(line[name_index:name_index + 3])
                                    # print('top 50 name found in:', line)
                        if line[name_index:name_index + 3] not in manager_names:
                            for name in tw_lastnames_200:
                                if name in line:
                                    name_index = line.find(name)
                                    manager_names.append(line[name_index:name_index + 3])
                                    # print('top 200 name found in:', line)

                    manager_names = [x for x in manager_names if (not set(x).intersection(names_ban) and len(x) > 2)]

                    manager_names_temp = manager_names
                    manager_names = []
                    for nm in manager_names_temp:
                        bool_list_mng = []
                        for wr_kw in names_incor + config['manager_avoid']:
                            bool_list_mng.append(wr_kw in nm)
                        if not any(bool_list_mng):
                            manager_names.append(nm)

                    if reloop:
                        break
                    if len(manager_names) > 0:
                        break
                    elif len(manager_names) == 0:
                        reloop = True

                if len(set(manager_names)) == 1:
                    manager_names = [manager_names[0]]

                #print(manager_names)

                if len(manager_names) == 1:
                    report_output_manager = manager_names[0]
                    manager_found_files += 1
                elif len(manager_names) > 1:
                    print('multiple found:', manager_names)
                    report_output_manager = manager_names[0]  # filter NEEDED?
                    manager_found_files += 1



            print('Manager:', report_output_manager)

            # </editor-fold>

            # <editor-fold desc='Bank'>
            bank_lines = []
            report_output_bank = 'Bank NF'


            for table in dfs:
                data_arr = table.to_numpy().tolist()
                for i, line in enumerate(data_arr):
                    for kw in config['bank_keywords']:
                        for c, cell in enumerate(line):
                            if type(cell) == str:
                                str_index_bank = cell.find(kw)
                                if str_index_bank != -1:
                                    bank_lines.append(line[c:])
                                    break

            for i, l in enumerate(bank_lines[:]):
                bank_lines[i] = [x for x in l if type(x) == str]
                temp_bank_line = []
                for j, x in enumerate(bank_lines[i]):
                    temp_bank_line += x.split(' ')
                bank_lines[i] = temp_bank_line

            bank_lines = trim(bank_lines)
            digits = list('0123456789%')
            bank_lines = [s for s in bank_lines if not any([dig in s for dig in digits])]
            bank_lines = [s for s in bank_lines if len(s) > 1]

            output_bank = ''
            for l in bank_lines:
                if ('銀行' in l or '商銀' in l or '企銀' in l or '機構' in l) and (
                        '保管銀行' not in l and '保管機構' not in l):
                    report_output_bank = l
                    bank_found_files += 1
                    break

            # print(type(report_output_bank))

            if type(report_output_bank) == list and report_output_bank[0] == '保管銀行':
                report_output_bank = report_output_bank[1]

            if report_output_bank == 'Bank NF':
                for line in pdf_content_all:
                    for kw in config['bank_keywords']:
                        if kw in line:
                            bank_kw_index = line.find(kw)
                            bank_lines.append(line[bank_kw_index:])

                for i, line in enumerate(bank_lines[:]):
                    bank_lines[i] = bank_lines[i].replace(' ', '')
                    bank_lines[i] = bank_lines[i].replace('保管費', '')
                    bank_lines[i] = bank_lines[i].replace('/', '')
                    bank_lines[i] = bank_lines[i].replace('銀行', '銀行;')
                    bank_lines[i] = bank_lines[i].replace('有限公司', '有限公司;')
                    bank_lines[i] = bank_lines[i].replace('部', '部;')
                    bank_lines[i] = bank_lines[i].replace('保管銀行;', '保管銀行|')
                    bank_lines[i] = bank_lines[i].replace('保管機構', '保管機構|')
                    bank_lines[i] = bank_lines[i].replace('|:', '|')
                    bank_lines[i] = bank_lines[i].replace('|：', '|')

                    bank_lines[i] = bank_lines[i].replace('|', ';')

                bank_lines_split = []
                bank_suffixes = ['銀行', '商銀', '企銀', '機構', '有限公司', '部']
                for line in bank_lines:
                    for spt_l in line.split(';'):
                        if spt_l != '' and any(x in spt_l for x in bank_suffixes) or 0 < len(spt_l) <= 4:
                            bank_lines_split.append(spt_l)

                if len(bank_lines_split) > 2:
                    if '有限公司' in bank_lines_split[2] or '部' in bank_lines_split[2]:
                        if len(bank_lines_split[2]) < 10:
                            bank_lines_split[1] = bank_lines_split[1] + bank_lines_split[2]
                        bank_lines_split.pop(2)
                if len(bank_lines_split) > 2:
                    if len(bank_lines_split[2]) <= 4:
                        bank_lines_split.pop(2)

                bank_lines_split = list(dict.fromkeys(bank_lines_split))
                bank_lines_split = [s for s in bank_lines_split if s != '成立日期']

                if len(bank_lines_split) == 2 and bank_lines_split[0] in ['保管銀行', '保管機構']:
                    report_output_bank = bank_lines_split[1]

                    if len(report_output_bank) > 17:
                        temp_bank_L = ''
                        for char in reversed(report_output_bank):
                            if char not in names_ban:
                                temp_bank_L = char + temp_bank_L
                            else:
                                break
                        report_output_bank = temp_bank_L

                    bank_found_files += 1


                elif len(bank_lines_split) == 1 and bank_lines_split[0] in ['保管銀行', '保管機構']:
                    report_output_bank = 'Bank NF'
                elif bank_lines_split == []:
                    report_output_bank = 'Bank NF'
                else:
                    report_output_bank = bank_lines_split

            # print('\n', file, '\nbank', bank_lines)
            # print('bank splt', bank_lines_split)
            print('Bank:', report_output_bank)
            # </editor-fold>

            # <editor-fold desc='Main Shares Held'>

            shares_lines = []
            shares_pairs = []
            final_shares_no = None

            for table_no, table in enumerate(dfs):

                data_arr = table.to_numpy().tolist()
                # print(data_arr)

                # print('data arr', table_no, data_arr)

                temp_data_arr = []

                for l in data_arr:
                    # print('O', l)
                    temp_data_arr.append([s for s in l if str(s) != 'nan'])
                    # print('N', l)

                data_arr = temp_data_arr[:]
                data_arr = [l for l in data_arr if l]  # empty list have False boolean value

                # print('data arr no nan', data_arr)

                shares_no = 'initialise'
                for i, l in enumerate(data_arr):  # l is line
                    for kw in config['shares_keywords_five']:
                        for c, cell in enumerate(l):
                            if type(cell) == str:
                                str_index_shares = cell.find(kw)
                                if str_index_shares != -1:
                                    shares_lines += data_arr[i:]
                                    shares_no, final_shares_no = 5, 5
                                    break

                    if shares_no == 'initialise':
                        for kw in config['shares_keywords_ten']:
                            for c, cell in enumerate(l):
                                if type(cell) == str:
                                    str_index_shares = cell.find(kw)
                                    if str_index_shares != -1:
                                        shares_lines += data_arr[i:]
                                        shares_no, final_shares_no = 10, 10
                                        break

                    if shares_no == 'initialise':
                        for kw in config['shares_keywords_misc']:
                            for c, cell in enumerate(l):
                                if type(cell) == str:
                                    str_index_shares = cell.find(kw)
                                    if str_index_shares != -1:
                                        shares_lines += data_arr[i:]
                                        shares_no, final_shares_no = -1, -1
                                        break

            shares_lines_keep = []

            for i, l in enumerate(shares_lines):
                l = [str(s) for s in l]
                shares_row_comb = ''.join(l)
                shares_kw_list = list('%0123456789') + config['shares_filter_kw']
                for kw in shares_kw_list:
                    if kw in shares_row_comb:
                        shares_lines_keep.append(l)
                        break
            shares_lines = shares_lines_keep[:]

            # print('post filt:', shares_lines)

            for i, l in enumerate(shares_lines):
                if len(l) == 2 and l[1].replace('%', '').replace('.', '').isdigit() and l[1] != l[1].replace('.',
                                                                                                             ''):
                    l[0] += (' ' + l[1])
                    l.remove(l[1])

            #  print('post comb:', shares_lines)

            regex_percentage = r'\b\d{1,2}\.\d{1,2}%{0,1}'
            for i, l in enumerate(shares_lines):
                shares_lines[i] = [re.sub(regex_percentage, r'\g<0>_split_', s).split('_split_') for s in l]
                shares_lines[i] = sum(shares_lines[i], [])
                shares_lines[i] = [s for s in shares_lines[i] if s != '']

            shares_lines = sum(shares_lines, [])

            shares_lines = [l for l in shares_lines if not re.search(r'20(?:1[0-9]|2[1-9])', l)]  # filter out dates
            shares_lines = [l for l in shares_lines if not re.search(r'11[1-5]', l)]
            shares_lines = [l for l in shares_lines if not re.search(r'\d{1,2}\\\d{1,2}\\\d{1,2}', l)]

            for sym in [',', '。', '、', ';', '月 ', '年 ']:
                shares_lines = [l.split(sym) for l in shares_lines]
                shares_lines = sum(shares_lines, [])

            #  print('post flat:', shares_lines)

            for i, s in enumerate(shares_lines[:]):
                if s.replace('%', '').replace('.', '').isdigit() and re.search(regex_percentage, s) and i != 0:
                    if not re.search(regex_percentage, shares_lines[i - 1]):
                        shares_lines[i - 1] += (' ' + shares_lines[i])

            shares_lines = [s for s in shares_lines if re.search(regex_percentage, s)]
            # shares_lines = [s for s in shares_lines if not any(kw in s for kw in config['shares_avoid'])]

            # print('post filt2:', shares_lines)

            shares_lines = [s for s in shares_lines if
                            len(re.sub(regex_percentage, '', s).replace(' ', '')) > 0]
            shares_lines = list(dict.fromkeys(shares_lines))  # del duplicates

            #  print('post filt3:', shares_lines)

            for i, s in enumerate(shares_lines):
                shares_dict = {}
                shares_dict['value'] = re.search(regex_percentage, s).group()
                shares_dict['name'] = re.sub(regex_percentage, '', s)

                # shares_dict['table_no'] = table_no

                shares_dict['value'] = shares_dict['value'].replace('%', '')

                if shares_dict['name'][-1] == ' ':
                    shares_dict['name'] = shares_dict['name'][:-1]
                if shares_dict['name'][0] == ' ':
                    shares_dict['name'] = shares_dict['name'][1:]

                if len(shares_dict['name']) <= 2:
                    continue

                shares_pairs.append(shares_dict)

            for d in shares_pairs:
                d['name'] = re.sub(r'(\d{1,2}\s){6}', '', d['name'])
                d['name'] = re.sub(r'(- ){3}', '', d['name'])
                d['name'] = d['name'].replace('771.95', '')

                if d['name'].endswith('(Y股累'):
                    d['name'] += ')'

            all_prefix = [d['name'][:2] for d in shares_pairs]

            # final_shares_no determines 五大 or 十大
            if final_shares_no == -1:
                if len(shares_pairs) <= 8:
                    final_shares_no = 5
                else:
                    final_shares_no = 10

            enum_space5 = ['1 ', '2 ', '3 ', '4 ', '5 ']
            enum_dot5 = ['1.', '2.', '3.', '4.', '5.']
            enum_all5 = enum_space5 + enum_dot5

            enum_space10 = enum_space5 + ['6 ', '7 ', '8 ', '9 ', '10']
            enum_dot10 = enum_dot5 + ['6.', '7.', '8.', '9.', '10']
            enum_all10 = enum_space10 + enum_dot10

            for i, d in enumerate(shares_pairs[:]):
                try:
                    d['value'] = Decimal(d['value'])
                except ValueError:
                    shares_pairs.remove(d)
                    continue
                # print(d)

                if all(prefix_num in all_prefix for prefix_num in enum_space10):
                    if d['name'][:2] not in enum_space10:
                        shares_pairs.remove(d)
                        # print('^^ DELETED ^^')
                        continue
                elif all(prefix_num in all_prefix for prefix_num in enum_dot10):
                    if d['name'][:2] not in enum_dot10:
                        shares_pairs.remove(d)
                        # print('^^ DELETED ^^')
                        continue
                elif all(prefix_num in all_prefix for prefix_num in enum_space5):
                    if d['name'][:2] not in enum_space5:
                        shares_pairs.remove(d)
                        # print('^^ DELETED ^^')
                        continue
                elif all(prefix_num in all_prefix for prefix_num in enum_dot5):
                    if d['name'][:2] not in enum_dot5:
                        shares_pairs.remove(d)
                        # print('^^ DELETED ^^')
                        continue

                # final_shares_no determines 五大 or 十大
                if final_shares_no == -1:
                    if len(shares_pairs) <= 9:
                        final_shares_no = 5
                    else:
                        final_shares_no = 10

                conditions = [
                    len(d['name']) < 7 and not d['name'] in config['shares_exceptions_exact'],  # 7 subject to change
                    (d['name'][0], d['name'][-1]) in [('(', ')'), ('（', '）')],
                    float(d['value']) == 0,
                    d['name'][0] in [',', '.', '，', '、', '。'],
                    d['name'][-1] in [',', '，', '、', '。', '+', '%', '-'],
                    any(kw in d['name'] for kw in config['shares_avoid_kw']),
                    any(d['name'].endswith(kw) for kw in config['shares_avoid_end'])
                ]

                if d['name'] in config['shares_avoid_exact']:
                    shares_pairs.remove(d)
                    # print('^^ DELETED ^^')
                    continue

                elif final_shares_no == 5:
                    if d['name'][:2] not in enum_all5:
                        if any(conditions):
                            shares_pairs.remove(d)
                            # print('^^ DELETED ^^')
                            continue
                elif final_shares_no == 10:
                    if d['name'][:2] not in enum_all10 or d['name'][:3] == '100':
                        if any(conditions):
                            shares_pairs.remove(d)
                            # print('^^ DELETED ^^')
                            continue

                # print('^^ kept ^^')

            shares_values = [d['value'] for d in shares_pairs]

            if final_shares_no == 5 and len(shares_pairs) > 5:
                shares_values_first5 = [d['value'] for d in shares_pairs[:5]]
                shares_values_last5 = [d['value'] for d in shares_pairs[-5:]]
                if shares_values != sorted(shares_values, reverse=True):
                    if shares_values_first5 == sorted(shares_values_first5, reverse=True):
                        shares_pairs = shares_pairs[:5]
                        print('FIRST 5 SHARES TAKEN')
                    elif shares_values_last5 == sorted(shares_values_last5, reverse=True):
                        shares_pairs = shares_pairs[-5:]
                        print('LAST 5 SHARES TAKEN')
                elif shares_values_first5 == sorted(shares_values_first5, reverse=True):
                    shares_pairs = shares_pairs[:5]
                    print('FIRST 5 SHARES TAKEN')

            if final_shares_no == 10 and len(shares_pairs) > 10:
                shares_values_first10 = [d['value'] for d in shares_pairs[:10]]
                shares_values_last10 = [d['value'] for d in shares_pairs[-10:]]
                if shares_values != sorted(shares_values, reverse=True):
                    if shares_values_first10 == sorted(shares_values_first10, reverse=True):
                        shares_pairs = shares_pairs[:10]
                        print('FIRST 10 SHARES TAKEN')
                    elif shares_values_last10 == sorted(shares_values_last10, reverse=True):
                        shares_pairs = shares_pairs[-10:]
                        print('LAST 10 SHARES TAKEN')

            shares_values = [d['value'] for d in shares_pairs]


            print('Shares:')
            for d in shares_pairs:
                print(d)

            shares_corr = 'N'
            if len(shares_pairs) > 0:
                all_true = True
                if len(shares_pairs) == 5 and final_shares_no == 10:
                    final_shares_no = 5
                if len(shares_pairs) == final_shares_no:
                    pass
                    #print('Correct length:', len(shares_pairs), f'({final_shares_no})')
                elif len(shares_pairs) < final_shares_no:
                    all_true = False
                    #print('Incorrect length (LESS):', len(shares_pairs), f'({final_shares_no})')
                elif len(shares_pairs) > final_shares_no:
                    all_true = False
                    #print('Incorrect length (MORE):', len(shares_pairs), f'({final_shares_no})')

                if shares_values == sorted(shares_values, reverse=True):
                    pass
                    #print('Correct descending order')
                else:
                    all_true = False
                    #print('Incorrect descending order')
                    if len(shares_pairs) == final_shares_no == 10:
                        all_true = True

                if sum(shares_values) <= 100:
                    pass
                    #print('Correct % sum:', sum(shares_values), '<= 100')
                elif sum(shares_values) > 100:
                    #print('Incorrect % sum:', sum(shares_values), '> 100')
                    all_true = False

                if all_true:
                    pass
                    print('Shares correct! \n')
                    shares_corr = 'Y'
                    shares_extracted_files += 1
                else:
                    shares_corr = 'N'
                    print('Something went wrong... \n')

                for d in shares_pairs:
                    d['value'] = float(d['value'])
                    format(d['value'], '.2f')

            else:
                print('Empty shares!!')

            # </editor-fold>
            # '''

            '''
            # <editor-fold desc='RiskL'>
            # 風險等級（很多沒有）
            # LATER (seldom have)
            # </editor-fold>
    
            # <editor-fold desc='Asset Allocation'>
            # 資產配置 AstAl 
            # LATER (difficult to implement)
            # </editor-fold>
            '''

            ##print('Date of data collection:', report_output_date)
            # print('Date status:', date_status)
            ##print('AUM output:', report_output_AUM)

            # print('aum_lines:', aum_lines)
            # print('aum_line_final:', aum_line_final)
            # print('aum_line_final_filt:', aum_line_final_filt)
            # print('aum_line_final_filt_split:', aum_line_final_filt_split)
            # print('aum_line_final_filt_nums:', aum_line_final_filt_nums)
            ##print('Product name:', report_output_prodname)
            ##print('Manager name:', report_output_manager)
            ##print('Bank:', report_output_bank)

            file_temp = file.replace('.pdf', '').split('_', 1)
            company_code = file_temp[0]
            prod_code = file_temp[1]

            if csv_gen:
                report_output = [file, company_code, prod_code, report_output_prodname, report_output_date,
                                 report_output_AUM, report_output_manager, report_output_bank, shares_corr, shares_pairs]
                csv_out.append(report_output)

        # '''
    except Exception as e:
        print(e)
        if config['export_error']:
            error_output = str(file) + ': Error - ' + str(e)
            error_fp = config['error_fp'] + 'error' + '_' + nowstr + '.txt'
            with open(error_fp, 'a', encoding='utf_16') as f:
                f.write(error_output + "\n")
        '''

    except ZeroDivisionError: #placeholder
        pass
        '''

###print @ end
print('total files =', total_files)

if total_files != 0:
    #print('\n AUM statistics:')
    print('AUMs extracted =', extracted_files_aum, f'({extracted_files_aum / total_files * 100:.2f}%)')
    #print('\n Shares statistics:')

    ##print('\n Date statistics:')
    ##print('dates extracted =', extracted_files_date)
    ##print('dates correct =', correct_files_date)
    ##print('dates incorrect =', extracted_files_date - correct_files_date)
    ##print('dates cannot be found =', total_files - extracted_files_date)
    # print('\n', 'DLLH: raw, removed duplicates, recency filter, month edge days')
    ##print('dates extracted % =', extracted_files_date / total_files)
    ##if correct_files_date != 0:
    ##print('dates correct % out of all =', correct_files_date / total_files)
    # print('dates correct % out of extracted =', correct_files_date / extracted_files_date)
    print('Manager extracted =', manager_found_files, f'({manager_found_files / total_files * 100:.2f}%)')
    print('Bank extracted =', bank_found_files, f'({bank_found_files / total_files * 100:.2f}%)')
    print('Shares extracted =', shares_extracted_files, f'({shares_extracted_files / total_files * 100:.2f}%)')

'''
if total_files == 0:
    for file in os.listdir(dir_path + '//read//'):
        os.rename(dir_path + '//read//' + file, dir_path + '//unread//' + file)
    for file in os.listdir(dir_path + '//cannot_be_read//'):
        os.rename(dir_path + '//cannot_be_read//' + file, dir_path + '//unread//' + file)
'''

if csv_gen and csv_out != []:
    with open(csv_name, 'a+', encoding='utf_16', newline='') as csv_f:
        csv_f.seek(0)
        reader = csv.reader(csv_f, delimiter=',')
        try:
            fst_row = next(reader)
        except StopIteration:
            # csv is empty!
            # intialise title row
            csv_out.insert(0,
                           ['File name', 'Company code', 'Product code', 'Product name', 'Date', 'AUM', 'Manager', 'Bank',
                            'Shares correct?', 'Shares list'])
        writer = csv.writer(csv_f, dialect='excel')
        writer.writerows(csv_out)
        print('\n csv written! (' + str(raw_report_date) + ')')

print('\n\n time elapsed: {:.2f}s'.format(time.time() - start_time))

# sys.stdout.close()