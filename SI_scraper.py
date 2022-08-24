from PyPDF2 import PdfReader

from datetime import date, datetime, timedelta

import os
import sys

from decimal import Decimal

import yaml
import csv

from string import punctuation

import time
start_time = time.time()


with open("SI_scrape_config.yaml", 'r', encoding='utf-8') as conf_f:
    config = yaml.safe_load(conf_f)
    print(config)
    #print(yaml.dump(config))


csv_gen = config['export_csv']
# bool to indicate whether to export scraped data into a csv or not.


def last_day_of_month(dt):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = dt.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return (next_month - timedelta(days=next_month.day)).day


# set raw_report_date int to choose input batch (month); 202205 for May 2022, 202211 for Nov 2022 etc.
raw_report_date = config['report_month']
report_year = int(str(raw_report_date)[:4])
report_month = int(str(raw_report_date)[4:])
report_date = date(report_year, report_month, last_day_of_month(date(report_year, report_month, 1)))
# year, month and date of batch of reports. Also part of filepath (name of folder containing the pdfs)

if csv_gen:
    csv_out = [['filename', 'prod name', 'date', 'AUM', 'manager name', 'bank']]#, 'currency']]
    # initialize csv file

'''
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open('output.txt', 'w', encoding='utf_16')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # python 3 compatibility
        pass

    def close(self):
        self.log.close()


sys.stdout = Logger()
'''

# Logger class exported terminal output to a txt file. I used it for debugging



def readpdf(filepath):
    filename = os.path.basename(filepath)

    parsed = PdfReader(filepath)

    # pdf_metadata = parsed.metadata

    first_page_text = parsed.pages[0].extract_text()

    all_pages_text = first_page_text

    number_of_pages = parsed.getNumPages()
    if number_of_pages > 1:
        for page_no in range(1, number_of_pages):
            page = parsed.getPage(page_no)
            page_content = page.extractText()
            all_pages_text += '\n' + page_content

    txt_name = config['scraped_txt_fp'] + str(
        raw_report_date) + '//' + filename + '_raw.txt'

    '''
    with open(txt_name, 'w', encoding='utf_16') as f:
       f.write(all_pages_text)
    '''
    # ^ created txt file of pdf content if needed, so that i can view the raw scraped text for debugging

    return all_pages_text


twomonthsago = report_date - timedelta(days=63)


# Jul and Aug both have 31 days and are consecutive. 31+31+1=63


def filterdates(date_list):
    date_list_len_history = []
    date_list_len_history.append(len(date_list))

    date_list = list(dict.fromkeys(date_list))
    # remove duplicates from list

    date_list_len_history.append(len(date_list))

    dt_to_remove = []

    for dt in date_list:
        if dt.date() > report_date or dt.date() < twomonthsago:
            dt_to_remove.append(dt)

    for dt in dt_to_remove:
        date_list.remove(dt)

    dt_to_remove = []

    date_list_len_history.append(len(date_list))

    edge_days = []
    # 28, 29, 30, 31? edit if necessary

    for dt in date_list:
        # print(dt.month)
        # print((dt + timedelta(days=1)).month)
        if dt.day > 5:
            # if dt.month != (dt + timedelta(days=2)).month:
            edge_days.append(dt)

    date_list_len_history.append(len(edge_days))

    # print("date list len history:", date_list_len_history)
    return (edge_days)


def find_word(content, strlist, badstrlist, output):
    lines_w_digits = []
    lines_raw = []
    lines = content
    for line in lines:
        n_line = ''.join(c if c.isdigit() else c if c == " " else ' ' for c in line)
        n_line = " ".join(n_line.split())
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


total_files = 0
extracted_files_date = 0
correct_files_date = 0
not_extracted_files_aum = 0
not_extracted_files_curr = 0
manager_found_files = 0
bank_found_files = 0
names_empty = 0

# directory structure note:
# my main directory is the Desktop folder "Sharpinvest"
# within it i have the subfolder "monthly_report_pdfs" to store the raw .pdfs
# in that folder there are subfolders named "YYYYMM" (eg 202205, 201911) to indicate the year/month of reports being scraped


dir_path = config['report_pdf_fp'] + str(raw_report_date) #a.encode('unicode_escape')
dir_path_txt = config['scraped_txt_fp'] + str(raw_report_date)
ext = 'pdf'

curr_year = int(date.today().year)
recent_years = [str(curr_year), str(curr_year - 1), str(curr_year - 1911), str(curr_year - 1911 - 1)]

recent_years_raw = [str(curr_year), str(curr_year - 1)]
for year in recent_years_raw:
    recent_years.append(year[:2] + ' ' + year[2:])
    # fix edge cases where 20 22, 20 19 is scraped instead of 2022, 2019

for file in os.listdir(dir_path + '//unread//'):
    total_files += 1
    fp = dir_path + '//unread//' + file  # filepath

    # following two if statements were used when i was testing certain/select few files.
    # if True is to retain the indentation structure
    # if file in ['AVIVA_FZ90.pdf']:
    if True:

        # <editor-fold desc="Date">
        # DATE COLLECTION START

        report_output_date = 'No full date found'

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
        pdf_content_noChi = pdf_content.replace(' ', '')
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
            if len(l) in [4, 5] and l[0] == "2":
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

        for i, l in enumerate(digit_lines_conc):
            if l[2] == ' ':
                digit_lines_conc[i] = l.replace(" ", "", 1)

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

        # date_ym = '      '
        # 民國years mingguo
        if digit_lines_conc_filt == []:
            for l in digit_lines_conc:
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
            report_output_date = "DATE NOT FOUND"
        else:
            date_ym_dt = []

            for dt in digit_lines_conc_filt:
                date_split = dt.split(' ')
                if len(date_split) == 3:
                    if 1 <= int(date_split[1]) <= 12:
                        if 1 <= int(date_split[1]) <= 31:
                            date_ym_dt.append(date(int(date_split[0]), int(date_split[1]), int(date_split[2])))
                        else:
                            date_ym_dt.append(date(int(date_split[0]), int(date_split[1]),
                                                   last_day_of_month(
                                                       date(int(date_split[0]), int(date_split[1]), 1))))
                elif len(date_split) == 2:
                    if 1 <= int(date_split[1]) <= 12:
                        date_ym_dt.append(date(int(date_split[0]), int(date_split[1]),
                                               last_day_of_month(date(int(date_split[0]), int(date_split[1]), 1))))

            for i, dt in enumerate(date_ym_dt):
                if dt.year in [report_year - 1910, report_year - 1911, report_year - 1912]:
                    date_ym_dt[i] = date_ym_dt[i].replace(year=dt.year + 1911)

            dt_to_remove = []
            for dt in date_ym_dt:
                if dt > report_date or dt < twomonthsago:
                    dt_to_remove.append(dt)

            for dt in dt_to_remove:
                date_ym_dt.remove(dt)

            if date_ym_dt == []:
                report_output_date = 'No full date found'
            else:
                report_output_date = sorted(date_ym_dt, reverse=True)[0]

        if type(report_output_date) is not str:
            extracted_files_date += 1

        elif report_output_date == 'No full date found':
            date_status += "[date not found] "

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

        '''
        else:
            #date_status += "[not 531] "
            date_status += "[not 4 end] "
            pass
        '''
        # DATE COLLECTION END
        # </editor-fold>

        # <editor-fold desc="AUM">
        # AUM COLLECTION START

        pdf_content_all = []
        pdf_content_all = readpdf(fp)
        # pdf_content_all is str

        pdf_content_all = pdf_content_all.split('\n')

        aum_keywords = config['aum_keywords']
        aum_keywords_secondary = config['aum_keywords_1']
        aum_keywords_avoid = config['aum_avoid']  # 費規模 because 經理費規模, 保管費規模

        aum_lines = find_word(pdf_content_all, aum_keywords, aum_keywords_avoid, 'r')

        chars_to_keep = '帳戶資產月底規模0123456789usdUSD$NT$TWDtwdKNTDMmilBbilAUDaudCNY澳幣新臺人民幣美元台金十拾百佰千仟萬億兆:.,(/)%'
        # txtfp_aum or pdf_content_all

        chars_to_keep_list = list(chars_to_keep)

        for i, l in enumerate(aum_lines):

            aum_lines[i] = l.replace(" ", "").replace("\n", "")

            aum_lines[i] = ''.join(char for char in l if char in chars_to_keep_list)
            # aum_lines[i] = ''.join(char if char in chars_to_keep_list else ' ' for char in l)

            aum_lines[i] = " ".join(aum_lines[i].split())
            aum_lines[i] += 'xxxxx'  # padded with 'x' to avoid index out of range

            for char_i in reversed(range(len(aum_lines[i]) - 5)):
                if aum_lines[i][char_i] == '月':
                    if aum_lines[i][char_i + 1:char_i + 4] != '底規模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == '底':
                    if aum_lines[i][char_i - 1] != '月' or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '戶':
                    if aum_lines[i][char_i - 1] != '帳' or char_i == 0 or aum_lines[i][char_i + 1:char_i + 3] != '規模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == '帳':
                    if aum_lines[i][char_i + 1] != '戶':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                    elif aum_lines[i][char_i + 1:char_i + 4] != '戶規模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '規':
                    if aum_lines[i][char_i + 1] != '模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == '模':
                    if aum_lines[i][char_i - 1] != '規' or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '資':
                    if aum_lines[i][char_i + 1:char_i + 4] != '產規模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == '產':
                    if aum_lines[i][char_i - 1] != '資' or char_i == 0 or aum_lines[i][char_i + 1:char_i + 3] != '規模':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '金':
                    if aum_lines[i][char_i - 1] != '美' or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '人':
                    if aum_lines[i][char_i + 1:char_i + 3] != '民幣':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == '民':
                    if aum_lines[i][char_i - 1] != '人' or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '新':
                    if aum_lines[i][char_i + 1] not in ['臺', '台']:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] in ['臺', '台']:
                    if aum_lines[i][char_i + 1] != '幣':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == 'T':
                    if aum_lines[i][char_i + 1] not in ['W', 'D', '$']:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == 't':
                    if aum_lines[i][char_i + 1] not in ['w', 'd', '$']:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == 'W':
                    if aum_lines[i][char_i + 1] != 'D':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == 'w':
                    if aum_lines[i][char_i + 1] != 'd':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == 'S':
                    if aum_lines[i][char_i - 1] != 'U':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]
                elif aum_lines[i][char_i] == 's':
                    if aum_lines[i][char_i - 1] != 'u':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == 'i':
                    if aum_lines[i][char_i + 1] != 'l':
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == ')':
                    if aum_lines[i][char_i - 1] == '(' or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i - 1] + aum_lines[i][char_i + 1:]
                    elif not aum_lines[i][char_i - 1].isdigit():
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == '.':
                    if not aum_lines[i][char_i - 1].isdigit() or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

                elif aum_lines[i][char_i] == ',':
                    if not aum_lines[i][char_i + 1].isdigit() or char_i == 0:
                        aum_lines[i] = aum_lines[i][:char_i] + aum_lines[i][char_i + 1:]

            aum_lines[i] = aum_lines[i][:len(aum_lines[i]) - 5]

        aum_lines = [l for l in aum_lines if len(l) > 8]  # remove elem too short to be valid.
        aum_lines = [l for l in aum_lines if (''.join(c for c in l if c.isdigit()) != '')]
        if len(set(aum_lines)) == 1 and len(aum_lines) > 1:  # removes duplicates
            aum_lines = [aum_lines[0]]

        if len(aum_lines) == 1:
            aum_line_final = aum_lines[0]
        elif len(aum_lines) >= 1:
            combined_lines = ''
            for l in aum_lines:
                combined_lines += l
            if "帳戶規模" in combined_lines:
                for l in reversed(aum_lines):
                    if "帳戶規模" in l:
                        aum_line_final = l
            else:
                aum_line_final = aum_lines[0]  # placeholder; find a way to choose correct one
        elif len(aum_lines) == 0:
            aum_line_final = "AUM not found"
        else:
            aum_line_final = '?'

        '''
        usd_keywords = config['usd_keywords']
        twd_keywords = config['twd_keywords']
        aud_keywords = config['aud_keywords']
        cny_keywords = config['cny_keywords']
        eur_keywords = config['eur_keywords']
        
        report_output_currency = ''

        for word in usd_keywords:
            if aum_line_final.find(word) != -1:
                report_output_currency = 'usd'
        if report_output_currency == '':
            for word in twd_keywords:
                if aum_line_final.find(word) != -1:
                    report_output_currency = 'twd'
        if report_output_currency == '':
            for word in aud_keywords:
                if aum_line_final.find(word) != -1:
                    report_output_currency = 'aud'
        if report_output_currency == '':
            for word in cny_keywords:
                if aum_line_final.find(word) != -1:
                    report_output_currency = 'cny'
        if report_output_currency == '':
            for word in eur_keywords:
                if aum_line_final.find(word) != -1:
                    report_output_currency = 'eur'
        if report_output_currency == '':
            report_output_currency = 'unknown'
            not_extracted_files_curr -= 1
        
        '''
        report_output_AUM = []

        chars_to_keep_postcurr = '0123456789%.百佰千仟萬億兆MmilBbilK(（'
        chars_to_keep_postcurr_list = list(chars_to_keep_postcurr)

        aum_line_final_filt = ''.join(
            char if (char.isdigit() or char in chars_to_keep_postcurr_list) else '' if char in [','] else ' '
            for char in aum_line_final)

        aum_line_final_filt_temp = aum_line_final_filt
        for i, char in enumerate(aum_line_final_filt):
            if char in ['(', '（'] and i != len(aum_line_final_filt) - 1:
                if aum_line_final_filt[i + 1] in list('百佰千仟萬億兆'):
                    aum_line_final_filt_temp = aum_line_final_filt[:i] + aum_line_final_filt[i + 1:]
                    break
                elif aum_line_final_filt[i + 1:i + 3] == '20':
                    aum_line_final_filt_temp = aum_line_final_filt[:i] + ' ' + aum_line_final_filt[i + 1:]
                    break
        aum_line_final_filt = aum_line_final_filt_temp

        aum_line_final_filt = " ".join(aum_line_final_filt.split())

        if aum_line_final == "AUM not found":
            aum_line_final_filt_split = []
        else:
            aum_line_final_filt_split = aum_line_final_filt.split(" ")

        allnum_str = ''
        for numstr in aum_line_final_filt_split:
            allnum_str += numstr

        aum_line_final_filt_nums = []
        chars_to_keep_postcurr_filt = '0123456789.'
        chars_to_keep_postcurr_list_filt = list(chars_to_keep_postcurr_filt)

        aum_val = '0'

        if ''.join(c for c in allnum_str if c.isdigit()) == '':
            report_output_AUM = 'AUM not found'

        else:
            aum_line_final_filt_split_temp = aum_line_final_filt_split
            for numstr in aum_line_final_filt_split:
                if '%' in numstr:  # remove percentages (since AUM index cannot be a %)
                    aum_line_final_filt_split_temp.remove(numstr)
                elif numstr not in ["百萬", "佰萬", "千萬", "仟萬", "億", "十萬", "拾萬", "千", "仟", "百", "佰"] and [c for c in numstr if c.isdigit()] == []:
                    aum_line_final_filt_split_temp.remove(numstr)
            aum_line_final_filt_split = aum_line_final_filt_split_temp

            for i, n in enumerate(aum_line_final_filt_split):
                if n in ["百萬", "佰萬", "千萬", "仟萬", "億", "十萬", "拾萬", "千", "仟", "百", "佰"]:
                    pass
                else:
                    if aum_line_final_filt_split[i - 1] in ["百萬", "佰萬", "千萬", "仟萬", "億", "十萬", "拾萬", "千",
                                                            "仟", "百", "佰"] and len(
                                                            aum_line_final_filt_split) >= 2:
                        aum_line_final_filt_split.append(
                            aum_line_final_filt_split[i] + aum_line_final_filt_split[i - 1])

                    multiplier = 1

                    if "億" in n:
                        multiplier = 100000000
                    elif "千萬" in n:
                        multiplier = 10000000
                    elif "百萬" in n or "佰萬" in n:
                        multiplier = 1000000
                    elif "十萬" in n or "拾萬" in n:
                        multiplier = 100000
                    elif "萬" in n and "十" not in n and "拾" not in n and "百" not in n and "佰" not in n and "千" not in n and "仟" not in n:
                        multiplier = 10000
                    elif ("千" in n or "仟" in n) and "萬" not in n:
                        multiplier = 1000
                    elif ("百" in n or "佰" in n) and "萬" not in n:
                        multiplier = 100
                    elif ("十" in n or "拾" in n) and "萬" not in n:
                        multiplier = 10
                    elif "M" in n:
                        multiplier = 1000000
                    elif "Mil" in n:
                        multiplier = 1000000
                    elif "mil" in n:
                        multiplier = 1000000
                    elif "MIL" in n:
                        multiplier = 1000000
                    elif "B" in n:
                        multiplier = 1000000000
                    elif "Bil" in n:
                        multiplier = 1000000000
                    elif "BIL" in n:
                        multiplier = 1000000000
                    elif "bil" in n:
                        multiplier = 1000000000
                    elif "k" in n:
                        multiplier = 1000
                    elif "K" in n:
                        multiplier = 1000

                    if len(''.join(char for char in n if char == '.')) > 1:  # filter out items such as 12.456.6 etc
                        raw_n = ''.join(char for char in n if char in chars_to_keep_postcurr_list_filt)
                        str_index = raw_n.find('.')
                        if raw_n[str_index + 1:str_index + 3].isdigit():
                            n_filt = Decimal(raw_n[:str_index + 3])
                            n_filt *= multiplier
                            aum_line_final_filt_nums.append(n_filt)
                        pass

                    else:
                        raw_n = ''.join(char for char in n if char in chars_to_keep_postcurr_list_filt)
                        if raw_n != '':
                            n_filt = Decimal(raw_n)
                            n_filt *= multiplier
                            aum_line_final_filt_nums.append(n_filt)

            if len(aum_line_final_filt_nums) == 1 and aum_line_final_filt_nums[0] > 5000:
                report_output_AUM = aum_line_final_filt_nums[0]
            elif len(aum_line_final_filt_nums) > 1:
                for n in aum_line_final_filt_nums:
                    if 6 <= len(str(n)) <= 8:
                        if str(n)[:3] == "201" or str(n)[:3] == "202" or str(n)[:3] == "203":
                            pass
                        else:
                            if n > 5000:
                                report_output_AUM.append(n)
                    else:
                        if n > 5000:
                            report_output_AUM.append(n)

            if type(report_output_AUM) is list:
                if len(report_output_AUM) == 1:
                    report_output_AUM = report_output_AUM[0]

        if report_output_AUM == []:
            report_output_AUM = "AUM not found"
        elif type(report_output_AUM) is list:
            report_output_AUM = report_output_AUM[0]

        if report_output_AUM == "AUM not found":
            not_extracted_files_aum -= 1
            # os.rename(fp, dir_path + '//cannot_be_read//' + file)
        else:
             #os.rename(fp, dir_path + '//read//' + file)
            pass

        # AUM COLLECTION END
        # </editor-fold>

        # <editor-fold desc="Name">
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
        for i, line in enumerate(name_lines):
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

            name_lines_repl_el = []
            name_lines_repl_el += name_lines[i]
            name_lines_repl.append(name_lines_repl_el)

        for i, sen_sep in enumerate(name_lines_repl):
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

        for i, line in enumerate(name_lines_filt):
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
            names_empty += 1
            name_lines_filt = []
        elif len(name_lines_filt) == 1:
            report_output_prodname = [name_lines_filt[0]]
        elif len(name_lines_filt) > 1:
            report_output_prodname = name_lines_filt
            name_lines_filt_ordered = sorted(name_lines_filt, key=len)

            report_output_prodname = name_lines_filt_ordered

            if "" in name_lines_filt_ordered:
                name_lines_filt_ordered = name_lines_filt_ordered.remove("")

            # name_lines_filt_ordered = " ".join(name_lines_filt_ordered.split())

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
            report_output_prodname = 'Name not found'
        elif len(report_output_prodname) == 1:
            report_output_prodname = report_output_prodname[0]

        if len(report_output_prodname) <= 4:
            report_output_prodname = 'Name not found'

        # </editor-fold>

        # <editor-fold desc="Manager">
        # 經理人（一定有）

        tw_lastnames_10 = list('陳林黃張李王吳劉蔡楊')
        tw_lastnames_50 = list('許鄭謝洪郭邱曾廖賴徐周葉蘇莊呂江何蕭羅高潘簡朱鍾游彭詹胡施沈余盧梁趙顏柯翁魏孫戴')
        tw_lastnames_200 = ['張簡', '歐陽'] + list('范方宋鄧杜傅侯曹薛丁卓阮馬tai董温唐藍石蔣古紀姚連馮歐程湯黄田康姜白汪鄒尤巫鐘'
                                                           '黎涂龔嚴韓袁金童陸夏柳凃邵錢伍倪溫于譚駱熊任甘秦顧毛章史官萬俞雷粘饒闕'
                                                           '凌崔尹孔辛武辜陶段龍韋葛池孟褚殷麥賀賈莫文管關向包丘梅')
        #利裴樊房全佘左花魯安鮑郝穆塗邢蒲成谷常閻練盛鄔耿聶符申祝繆陽解曲岳齊籃應單舒畢喬龎翟牛鄞留季') + ['范姜']
        tw_lastnames_all = tw_lastnames_10 + tw_lastnames_50 + tw_lastnames_200

        names_ban = set(punctuation + ' 012346789abcdewfghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYUZ'
                                      ',.;:()：；，。（）「」【】＋、')
        names_incor = ['成立', '簡介月', '人壽', '全委', '經理', '管費', '管理', '全權', '投信', '全球',
                       '單位', '位淨', '波動', '大學', '機構', '預期', '顧問', '不', '一個', '投顧', '製造',
                       '勝率', '本月', '人', '金融', '與']
        manager_lines = []
        manager_names = []

        empty = True
        reloop = False
        while empty:
            for i, line in enumerate(pdf_content_all):
                for kw in config['manager_keywords']:
                    if kw in line:
                        mng_kw_index = line.find(kw)
                        if reloop:
                            manager_lines.append(pdf_content_all[i+1][:15])
                            #manager_lines.append(pdf_content_all[i-1][:15])
                            break
                        manager_lines.append(line[mng_kw_index:mng_kw_index+15])
                        break
            for line in manager_lines:
                name_index = -1
                for name in tw_lastnames_10:
                    if name in line:
                        name_index = line.find(name)
                        manager_names.append(line[name_index:name_index+3])
                        #print('top 10 name found in:', line)
                if line[name_index:name_index+3] not in manager_names:
                    for name in tw_lastnames_50:
                        if name in line:
                            name_index = line.find(name)
                            manager_names.append(line[name_index:name_index + 3])
                            #print('top 50 name found in:', line)
                if line[name_index:name_index+3] not in manager_names:
                    for name in tw_lastnames_200:
                        if name in line:
                            name_index = line.find(name)
                            manager_names.append(line[name_index:name_index + 3])
                            #print('top 200 name found in:', line)

            manager_names = [x for x in manager_names if (not set(x).intersection(names_ban) and len(x) > 2)]

            manager_names_temp = manager_names
            manager_names = []
            for nm in manager_names_temp:
                bool_list_mng = []
                for wr_kw in names_incor+config['manager_avoid']:
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

        #print(manager_names,'\n', file, '\n')

        if len(manager_names) == 1:
            manager_found_files += 1
            report_output_manager = manager_names[0]
        elif len(manager_names) == 0:
            report_output_manager = 'Manager name not found'
        elif len(manager_names) > 1:
            manager_found_files += 1
            report_output_manager = manager_names[0] # filter NEEDED?

        # </editor-fold>

        # <editor-fold desc="Bank">
        bank_lines = []

        for line in pdf_content_all:
            for kw in config['bank_keywords']:
                if kw in line:
                    bank_kw_index = line.find(kw)
                    bank_lines.append(line[bank_kw_index:])
        if len(bank_lines) == 0 and False:
            for line in pdf_content_all:
                for kw in config['bank_keywords_2']:
                    if kw in line:
                        bank_kw_index = line.find(kw)
                        bank_lines.append(line)


        for i, line in enumerate(bank_lines):
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
                if spt_l != '' and any(x in spt_l for x in bank_suffixes) or 0<len(spt_l)<=4:
                    bank_lines_split.append(spt_l)


        if len(bank_lines_split) > 2:
            if '有限公司' in bank_lines_split[2] or '部' in bank_lines_split[2] and len(bank_lines_split[2])<10:
                bank_lines_split[1] = bank_lines_split[1]+bank_lines_split[2]
                bank_lines_split.pop(2)
        if len(bank_lines_split) > 2:
            if len(bank_lines_split[2])<=4:
                bank_lines_split.pop(2)

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
            report_output_bank = 'Bank not found'
        else:
            report_output_bank = bank_lines_split


        print("\n bank", bank_lines)
        print("bank splt", bank_lines_split)
        # </editor-fold>



        '''
        # <editor-fold desc="RiskL">
        # 風險等級（很多沒有）
        # LATER (seldom have)
        # </editor-fold>

        # <editor-fold desc="Asset Allocation">
        # 資產配置 AstAl 
        # LATER (difficult to implement)
        # </editor-fold>

        # <editor-fold desc="Main Shares Held">
        # 主要投資標的（主要持股）
        # </editor-fold>
        '''






        ##print('\n\n-------------------------------------', total_files, '------------------------------------')
        ##print('File name: ', file)

        ##print('Date of data collection:', report_output_date)
        # print('Date status:', date_status)
        ##print("AUM output:", report_output_AUM)
        # print("Currency:", report_output_currency)

        # print("aum_lines:", aum_lines)
        # print("aum_line_final:", aum_line_final)
        # print("aum_line_final_filt:", aum_line_final_filt)
        # print("aum_line_final_filt_split:", aum_line_final_filt_split)
        # print("aum_line_final_filt_nums:", aum_line_final_filt_nums)

        # print('name_lines unreplaced:', name_lines_prereplace)
        # print('name_lines:', name_lines)
        ##print('Product name:', report_output_prodname)

        if csv_gen:
            csv_out.append([file, report_output_prodname, report_output_date, report_output_AUM, report_output_manager, report_output_bank])#, report_output_currency])

#  print @ end
print('total files =', total_files)
print('\n Date statistics:')
print('dates extracted =', extracted_files_date)
if correct_files_date != 0:
    print('dates correct =', correct_files_date)
    print('dates incorrect =', extracted_files_date - correct_files_date)
    print('dates cannot be found =', total_files - extracted_files_date)
print('\n', "DLLH: raw, removed duplicates, recency filter, month edge days")
if total_files != 0:
    print("dates extracted % =", extracted_files_date / total_files)
if correct_files_date != 0:
    print("dates correct % out of all =", correct_files_date / total_files)
    print("dates correct % out of extracted =", correct_files_date / extracted_files_date)
print('\n AUM statistics:')
print('AUMs extracted =', total_files + not_extracted_files_aum)
if total_files != 0:
    print('AUMs extracted % =', (total_files + not_extracted_files_aum) / total_files)
# print('currency extracted =', total_files + not_extracted_files_curr)
# print('currency extracted % =', (total_files + not_extracted_files_curr) / total_files)
print('total empty:', names_empty)

print('\n manager extracted:', manager_found_files)
print('manager extracted %:', manager_found_files/total_files)

print('\n bank extracted:', bank_found_files)
print('bank extracted %:', bank_found_files/total_files)



if total_files == 0:
    for file in os.listdir(dir_path + '//read//'):
        os.rename(dir_path + '//read//' + file, dir_path + '//unread//' + file)
    for file in os.listdir(dir_path + '//cannot_be_read//'):
        os.rename(dir_path + '//cannot_be_read//' + file, dir_path + '//unread//' + file)

if csv_gen:
    csv_name = config['output_csv_fp'] + "output_" + str(raw_report_date) + ".csv"
    with open(csv_name, 'w', encoding='utf_16', newline='') as csv_f:
        writer = csv.writer(csv_f, dialect='excel')
        writer.writerows(csv_out)
        print("\n csv written! (" + str(raw_report_date) + ")")


print("\n\n time elapsed: {:.2f}s".format(time.time() - start_time))

# sys.stdout.close()

