(last edited 30/08/2023)

This Python script/program was created for the Taiwanese "Sharpinvest” (精財網) service, to extract certain data from a batch of monthly reports (`.pdf` files) -- product name (產品名稱), AUM index (賬戶規模), manager name (經理人名稱), custodian bank (保管銀行), final date of collection (截止日期) and top five/ten Main Shares Held (前五/十大持股).

---
## Dependencies:
Before the program/script can be run, a Python runtime (3.7+) and a Java runtime (8+) has to be installed, the Java one needed since the `tabula-py` module is a Python wrapper of the `tabula-java` library.

A few dependencies need to be installed: 
- `pypdf`, `pdfminer.six` (to extract raw text)
- `tabula-py` (to extract tabular data)
- `PyYAML` (to read `.yaml` config file)

These modules can be installed with the package installer `pip` with the command:
`pip3 install pypdf pdfminer.six tabula-py PyYAML`
 
 Alternatively, run the batch file `pip_pyth_modules.cmd` (which runs the above package install command) to install.


The following built-in modules are also used:\
`os`, `sys`, `re`, `decimal`, `datetime`, `time`, `csv`

## Directory structure:
```
pyDataCapture
├── error_txt/
│   ├── error_[datetime1].txt
│   ├── error_[datetime2].txt
│   └── error_[datetime3].txt
├── logger_txt/
│   ├── output_[datetime1].txt
│   ├── output_[datetime2].txt
│   └── output_[datetime3].txt
├── output_csv/
│   └── output_[report_month]_[current_datetime].txt
├── read_txt/
│   ├── [PDFname1]_raw.txt
│   ├── [PDFname1]_tabDataframe.csv
│   ├── [PDFname2]_raw.txt
│   └── [PDFname2]_tabDataframe.csv
├── pip_pyth_modules.cmd
├── README.md
├── SI_scrape_config.yaml
└── SI_scraper.py
```

---
## Config file (`.yaml`) guide:

The config file (`SI_scrape_config.yaml`) should be placed in the same directory as the python `SI_scraper.py` file.

### Boolean Conditions
`export_csv` is a Boolean that indicates whether or not to export a `.csv` file (to `output_csv` folder) after the script/program completes. Should usually be set to `True` unless debugging.

`export_logger` is a Boolean that indicates whether or not the terminal output should be written to a `.txt` file (to `error_txt` folder). Should usually be set to `False` unless debugging.

`export_error` is a Boolean that indicates whether or not any errors/exceptions should be written to a `.txt` file (to `logger_txt` folder). Should usually be set to `False` unless debugging.

`move_pdfs` is a Boolean that indicates whether or not the `.pdf`s are moved to the `cannot_be_read`/`read` folder after the program ends.

`export_scanned_text` is a Boolean that indicates whether or not raw text extracted with `pdfminer.six` or `pypdf` is written to a `.txt` file (to `read_txt` folder). Should usually be set to `False` unless debugging.

`export_scanned_tabu` is a Boolean that indicates whether or not DataFrames (tables) extracted with `tabula-py` is written to a `.csv` file (to `read_txt` folder). Should usually be set to `False` unless debugging.

### Filepaths
`report_month` is the name of the folder that contains the reports for a certain month; in the format YYYYMM.

`report_pdf_fp` is the filepath of the folder containing the monthly report `.pdf`s. This folder should contain subfolders for each month (202205, 202206 etc.).

`output_csv_fp` is the filepath of the folder to output the final `.csv` file if `export_csv` is set to `True`.

`scraped_txt_fp` is the filepath of the folder to output raw `.txt`s and/or `.csv`s created by `pdfminer.six`/`pypdf` and/or `tabula-py` respectively if `export_scanned_text` or `export_scanned_text` is set to `True`. Should be disregarded unless debugging.

`logger_fp` is the filepath of the folder containing the terminal output `.txt`s if `export_logger` is set to `True`.

`error_fp` is the filepath of the folder containing the error message output `.txt`s if `export_error` is set to `True`.

#### Date extraction
`date_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the date of collection. If no valid lines are found then it searches for keywords in `date_keywords_1`, then `date_keywords_2`.

#### AUM extraction
`aum_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the AUM index. If no valid lines are found then it searches for keywords in `aum_keywords_1`.

`aum_avoid` are keywords that should be avoided when searching for AUM index. If a line in the pdf that contains a keyword from `aum_keywords` also contains a keyword from `aum_avoid`, it is ignored.

#### Product name extraction
`prodname_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the product name.

`prodname_avoid` are keywords that should be avoided when searching for product name. If a line in the pdf that contains a keyword from `prodname_keywords` also contains a keyword from `prodname_avoid`, it is ignored.

#### Manager name extraction
`manager_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the manager name.

`manager_avoid` are keywords that should be avoided when searching for manager name. If a line in the pdf that contains a keyword from `manager_keywords` also contains a keyword from `manager_avoid`, it is ignored.

#### Custodian bank extraction
`bank_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the custodian bank.

#### Major Shares Held extraction
`shares_keywords_five` and `shares_keywords_ten` is a list of strings that contain the keywords that the program uses to search for top five/ten shares. If none are found then it searches for keywords in `shares_keywords_misc`.\
If a keyword is found, all lines before it is discarded.

`shares_filter_kw` is a list of strings that contain the keywords searching for the share names.\
The potential share is disregarded if:
- It includes a keyword from `shares_avoid_kw`
- It ends with a keyword from `shares_avoid_end`
- It is exactly `shares_avoid_exact`

An exception is made if the share name is exactly `shares_exceptions_exact` (the reason for this is some PDF formatting cuts off the share name, only including the last 2-5 characters)


**Note**: Default values of each keyword list are included at the end of this README document.

---
In each folder containing monthly reports (YYYYMM), there should be 3 subfolders: `cannot_be_read` (無法讀取), `read` (已讀取) and `unread` (未讀取).

New reports to be processed by the program should be placed in the `unread` folder.

After the program completes, `.pdf` files where the AUM index cannot be read are moved to the `cannot_be_read` folder, and `.pdf` files where the AUM index can be read are moved to the `read` folder.
 

---
## Output
The outputted `.csv` file will be named as follows:

**`output_[report_month]_[current_datetime].csv`**

Where `[report_month]` is in the format YYYYMM, and `[current_datetime]` is in the format YYYYMMDDHHmmss.

The `.csv` file will have 10 columns:
1. File Name
2. Company (characters in file name before underscore)
3. Product Code (characters in file name after underscore)
4. Product Name
5. Date of Collection
6. AUM index
7. Manager name
8. Custodian Bank Name
9. Shares Validity (Y/N):\
This will be 'Y' only if all 3 of the following conditions are met:\
	a. Total number of shares extracted is 5 or 10 (depending on keyword)\
	b. The share percentages are descending\
	c. Sum of all share percentage is ≤100%


---
The default values for the keywords are as follows:
```
date_keywords: ['截至', '截止日期', '報告日期']
date_keywords_1: ['資料日期']
date_keywords_2: ['月', '年', '/', '報告']

aum_keywords: ['最新帳戶規模', '帳戶規模', '月底規模', '帳戶資產規模', '帳戶月底規模','最新規模', '帳戶型態/規模', '規模']
aum_keywords_1: ['億', '美金', 'USD', '美元', '台幣', 'TWD', 'twd', 'NT$', 'NTD','新臺幣', '臺幣', '新台幣', '澳幣', 'AUD', 'aud', '人民幣', 'CNY','cny', '百萬', '佰萬', '千萬', '仟萬', '萬', '十萬', '拾萬']
aum_avoid: ['費規模', '以下','每單位提解']

prodname_keywords: ['帳戶', '委託', '全委代操']
prodname_avoid: ['客服專線', '經理人報告', '六個月', '(全權委託帳戶之資產撥回機制來源可能為本金)','全權委託帳戶之資產撥回機制來源可能為本金','投資帳戶於資產撥回前未先扣除行政管理相關費用']

manager_keywords: ['經理人', '姓名']
manager_avoid: ['成立', '簡介月', '人壽', '全委', '經理', '管費', '管理', '全權', '投信','全球', '單位', '位淨', '波動', '大學', '機構', '預期', '顧問', '不','一個', '投顧', '製造', '勝率', '本月', '人', '金融', '與']

bank_keywords: ['保管銀行', '保管機構']

shares_keywords_five: ['前五大', '前 5大', '前5大','前 5 大','前5 大']
shares_keywords_ten: ['前十大']
shares_keywords_misc: ['基金名稱','持股明細','受益憑證名稱', 'ETF名稱','持有子基金','標的名稱','投資子標','名稱','類別','佔比','百分比']
shares_filter_kw: ['iShares', 'ISHARES', 'IShares', 'Ishares', '基金', 'ETF', '債券','INVESCO', 'Invesco', '宏利', '新臺幣', 'QQQ', 'TRUST SERIES','百達', 'Shares', 'shares', 'SHARES', '1000', '500', '600','IS', 'SPDR','FUND','成長股票']
shares_avoid_kw: ['下降至','回落至','下滑至','上漲','下跌','升至','維持','成⻑', '漲幅','小於','上調', '大漲','月增','降至','增加','增率','降溫','降低','上揚','專線','貶值','分別為','報酬率','比重','金額','波動率','淨值','經濟數據','總回報','已開發市場股票','規模','固定收益','全球型股票基金', '含資產撥回投資','全球型債券基金','不含英國','加回資產撥回)','亞太不含日本股票','全球新興市場股票',"\uf06c", '*','每單位淨資產價值<','年化標準差','經理費','於非投資等級之高風險債券且配息來源可能為本金)','http']
shares_avoid_end: ['等級債券', '貨幣債券','公司債券','國家政府債券']
shares_avoid_exact: ['新興市場股票基金','新興市場債券基金','ratio選取相對具有投資','且當前美國就業市','已開發市場債券基金','成熟市場股票基金','721.01美元','已開發國家股票','現金及貨幣基金','成立至今每單位累積配息','新興市場債券基金 現金','全球新興市場債劵','流動現金(現金)','4 .7%','資產配置美國股票','資產撥回組成表','前47.1/']
shares_exceptions_exact: ['現金','債券FUND','股票型','債券型']
```
