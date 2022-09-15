This Python script/program was created to extract certain data from  a batch of monthly reports (`.pdf` files) -- product name (產品名稱), AUM index (賬戶規模), manager name (經理人名稱), custodian bank (保管銀行), and final date of collection (截止日期).

`PyPDF2` is used to read raw text from the pdf file, and `tabula` is used to extract tabular data.

---
### Requirements:
Before the program/script can be run, a Python runtime (3.7+) and a Java runtime (8+) has to be installed, the Java one needed since the `tabula-py` module is a Python wrapper of the `tabula-java` library.

Additionally, a few dependencies need to be installed: 
- `PyPDF2` (to extract raw text)
- `tabula-py`, `tabula` (to extract tabular data)
- `PyYAML` (to read `.yaml` config file)

These modules can be installed with the package installer `pip` with the command:

 `pip3 install PyPDF2 tabula-py tabula datetime PyYAML`
 
 Alternatively, run the batch file `pip_pyth_modules.cmd` to install.


---
### Config file (.yaml) guide:

The config file (`SI_scrape_config.yaml`) should be placed in the same directory as the python `.py` file.

`export_csv` is a Boolean that indicates whether or not to export a `.csv` file after the script/program completes. Should usually be set to `True` unless debugging.

`move_pdfs` is a Boolean that indicates whether or not the `.pdf`s are moved to the `cannot_be_read`/`read` folder after the program ends.

`report_month` is the name of the folder that contains the reports for a certain month; in the format YYYYMM.

`scraped_txt_fp` is the filepath of the folder to output raw `.txt` files created by `PyPDF2`. Should be disregarded unless debugging.

`report_pdf_fp` is the filepath of the folder containing the monthly report `.pdf`s. This folder should contain subfolders for each month (202205, 202206 etc.).

`output_csv_fp` is the filepath of the folder to output the final `.csv` file.\
\
\
`date_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the date of collection. If no valid lines are found then it searches for keywords in `date_keywords_1`, then `date_keywords_2`.

`aum_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the AUM index. If no valid lines are found then it searches for keywords in `aum_keywords_1`.

`aum_avoid` are keywords that should be avoided when searching for AUM index. If a line in the pdf that contains a keyword from `aum_keywords` also contains a keyword from `aum_avoid`, it is ignored.

`prodname_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the product name.

`prodname_avoid` are keywords that should be avoided when searching for product name. If a line in the pdf that contains a keyword from `prodname_keywords` also contains a keyword from `prodname_avoid`, it is ignored.

`manager_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the manager name.

`manager_avoid` are keywords that should be avoided when searching for manager name. If a line in the pdf that contains a keyword from `manager_keywords` also contains a keyword from `manager_avoid`, it is ignored.

`bank_keywords` is a list of strings that contain the keywords that the program uses to search for lines containing the custodian bank.

`shares_keywords` and `shares_filter_kw` pertain to the yet incomplete major shareholder extraction, and can be disregarded.


Note: Default values of each keyword list are included at the end of this README document.

---
In each folder containing monthly reports (YYYYMM), there should be 3 subfolders: `cannot_be_read` (無法讀取), `read` (已讀取) and `unread` (未讀取).

New reports to be processed by the program should be placed in the `unread` folder.

After the program completes, `.pdf` files where the AUM index cannot be read are moved to the `cannot_be_read` folder, and `.pdf` files where the AUM index can be read are moved to the `read` folder.
 

---
The outputted `.csv` file will be named as follows:

**`output_[report_month]_[todaysdate].csv`**

Where `[report_month]` is in the format YYYYMM, and `[todaysdate]` is in the format YYYYMMDD.



---
The default values for the keywords are as follows:

```
date_keywords: ['截至', '截止日期', '報告日期']

date_keywords_1: ['資料日期']

date_keywords_2: ['月', '年', '/', '報告']

aum_keywords: ['最新帳戶規模', '帳戶規模', '月底規模', '帳戶資產規模', '帳戶月底規模', '最新規模', '帳戶型態/規模', '規模']

aum_keywords_1: ['億', '美金', 'USD', '美元', '台幣', 'TWD', 'twd', 'NT$', 'NTD', '新臺幣', '臺幣', '新台幣', '澳幣', 'AUD', 'aud', '人民幣', 'CNY', 'cny', '百萬', '佰萬', '千萬', '仟萬', '萬', '十萬', '拾萬']

aum_avoid: ['費規模']

prodname_keywords: ['帳戶', '委託', '全委代操']

prodname_avoid: ['客服專線', '經理人報告', '六個月', '(全權委託帳戶之資產撥回機制來源可能為本金)', '全權委託帳戶之資產撥回機制來源可能為本金', '投資帳戶於資產撥回前未先扣除行政管理相關費用']

manager_keywords: ['經理人', '姓名']

manager_avoid: ['成立', '簡介月', '人壽', '全委', '經理', '管費', '管理', '全權', '投信', '全球', '單位', '位淨', '波動', '大學', '機構', '預期', '顧問', '不', '一個', '投顧', '製造', '勝率', '本月', '人', '金融', '與']

bank_keywords: ['保管銀行', '保管機構']
```