import scrapy
import sqlite3

class Merger(scrapy.Spider):
    name = 'Merger'

    def __init(self, reset=False, **kwargs):
        self.reset = reset
        super().__init__(**kwargs)

    url = 'https://ec.europa.eu/competition/elojade/isef/index.cfm?fuseaction=dsp_result&policy_area_id=1'
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/83.0.4103.116 Safari/537.36"}
    form_data = {
        'list_region': '',
        'new_list_region': '',
        'policy_area_id': '2',
        'case_number': '',
        'case_title': '',
        'decision_date_from': '',
        'decision_date_to': '',
        'PICKUP_nace_code_myLabel': '',
        'nace_code': '',
        'nace_code_id': '',
        'webpub_date_from': '',
        'webpub_date_to': '',
        'label_gber': '',
        'includegberobj_id': '',
        'notification_date_from': '',
        'notification_date_to': '',
        'notification_date': '0_fromToPair',
        'council_reg_2': '1',
        'council_reg_1': '1',
        'simple_proc_yes': '1',
        'simple_proc_no': '1',
        'ojpub_date': '0_fromToPair',
        'ojpub_date_from': '',
        'ojpub_date_to': '',
        'merg_not_date': '0_fromToPair',
        'merg_not_date_from': '',
        'merg_not_date_to': '',
        'merg_deadline_date': '0_fromToPair',
        'merg_deadline_date_from': '',
        'merg_deadline_date_to': '',
        'doc_title': '',
        'antitrust': '1',
        'cartel': '1',
        'at_case_min': '',
        'at_case_max': '',
        'at_doc_date': '0_fromToPair',
        'at_doc_date_from': '',
        'at_doc_date_to': ''
    }

    def start_requests(self):
        conn = self.db_conn()
        if self.reset:
            self.drop_table(conn)
        self.create_table(conn)

        yield scrapy.FormRequest(
            url=self.url,
            headers=self.headers,
            formdata=self.form_data,
            callback=self.parse
        )

    def parse(self, response):
        conn = self.db_conn()
        if response.xpath('//input[@value="Next"]') is not None:
            page = response.css('.list').xpath('tr[not(descendant::td[contains(@id, "test")])]')
            from_row = response.xpath('//input[@value="Next"]/@onclick').re(r"\d{2,4}")
            self.form_data['fromrow'] = from_row
            self.form_data['fuseaction'] = 'dsp_result'
            self.form_data['sort'] = 'case_code asc'
            for row in page:
                case = row.css('.case')
                case_no = case.xpath('./span/text()').get()
                url = response.urljoin(case.xpath('./a/@href').get())
                dec_date = \
                    row.css('.decision').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                title = row.css('.title').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                self.insert_record(conn, [url, case_no, dec_date, title])
            return scrapy.FormRequest(
                url=response.url,
                formdata=self.form_data,
                headers=self.headers,
                callback=self.parse
            )

    def db_conn(self):
        try:
            conn = sqlite3.connect("records.db")
            return conn
        except sqlite3.Error as e:
            self.logger.error('Error occurred connecting to database: %s', e)

    def create_table(self, conn):
        curs = conn.cursor()
        curs.execute("CREATE TABLE IF NOT EXISTS records(id integer PRIMARY KEY, url text, case_no text, decision "
                     "text, title text)")
        conn.commit()
        curs.close()

    def drop_table(self, conn):
        curs = conn.cursor()
        curs.execute("DROP TABLE IF EXISTS records")
        conn.commit()
        curs.close()

    def insert_record(self, conn, record):
        curs = conn.cursor()
        curs.execute("INSERT INTO records(url, case_no, decision, title) VALUES (?, ?, ?, ?)", record)
        conn.commit()
        curs.close()