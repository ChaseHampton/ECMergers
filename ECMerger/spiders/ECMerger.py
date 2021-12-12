import scrapy
import sqlite3
from ..items import EcmergerItem


class Merger(scrapy.Spider):
    name = 'Merger'

    def __init(self, reset=False, skip=False, **kwargs):
        self.reset = reset
        self.skip = skip
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
        if 'total_rows' not in response.meta:
            total_rows = response.css('.navButton').xpath('./tr/td/text()')[-1].re(r"of (\d{,4})")[0]
        else:
            total_rows = response.meta['total_rows']

        if int(response.xpath('//input[@value="Next"]/@onclick').re(r"\d{2,4}")[0]) <= int(total_rows):
            page = response.css('.list').xpath('tr[not(descendant::td[contains(@id, "test")])]')
            from_row = response.xpath('//input[@value="Next"]/@onclick').re(r"\d{2,4}")
            self.form_data['fromrow'] = '8472'  # from_row
            self.form_data['fuseaction'] = 'dsp_result'
            self.form_data['sort'] = 'case_code asc'
            self.logger.info(f"starting at row: {self.form_data['fromrow']} of {total_rows}")
            for row in page:
                case = row.css('.case')
                case_no = case.xpath('./span/text()').get()  # get is the same as extract_first
                url = response.urljoin(case.xpath('./a/@href').get())
                dec_date = \
                    row.css('.decision').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                title = row.css('.title').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                self.insert_record(conn, [url, case_no, dec_date, title])
            return scrapy.FormRequest(
                url=response.url,
                formdata=self.form_data,
                headers=self.headers,
                meta={'total_rows': total_rows},
                callback=self.parse
            )
        else:
            page = response.css('.list').xpath('tr[not(descendant::td[contains(@id, "test")])]')
            for row in page:
                case = row.css('.case')
                case_no = case.xpath('./span/text()').get()
                url = response.urljoin(case.xpath('./a/@href').get())
                dec_date = \
                    row.css('.decision').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                title = row.css('.title').xpath('./text()').get().replace('\r', '').replace('\n', '').replace('\t', '')
                self.insert_record(conn, [url, case_no, dec_date, title])
            return self.search_cases(response)

    def search_cases(self, response):
        conn = self.db_conn()
        curs = conn.cursor()
        sql = "SELECT * FROM records"

        curs.execute(sql)

        for r in curs.fetchall():
            self.logger.info(f"looping through curs on case {r[2]}")
            yield scrapy.Request(
                url=str(r[1]),
                meta={
                    'policy': '',
                    'case_number': r[2],
                    'member_state': '',
                    'last_decision_date': r[3],
                    'title': r[4]
                },
                callback=self.parse_details
            )

    def parse_details(self, response):
        item = EcmergerItem()
        comps = [item.strip() for item in response.xpath('//div[@id="BodyContent"]//strong/a/text()').getall()]
        details = response.css('table.details')
        notif_date = details.xpath(
            '//tr/td[contains(text(), "Notification")]/following-sibling::td/text()').get().replace('\r', '').replace(
            '\n', '').replace('\t', '')
        prov_deadline = details.xpath(
            '//tr/td[contains(text(), "Provisional")]/following-sibling::td/text()').get().replace('\r', '').replace(
            '\n', '').replace('\t', '')
        prior_pub_node = details.xpath('.//tr/td[contains(text(), "Prior publication")]/following-sibling::td')
        journal_no = prior_pub_node.xpath('./a/text()').get()
        journal_date = prior_pub_node.xpath('./text()').re(r"\d{2}\.\d{2}\.\d{4}")[0]  # pulls out just the date
        nace_node = details.xpath('.//tr/td[contains(text(), "NACE")]/following-sibling::td')
        naces = [f"{i} {j}" for i, j in
                 zip(nace_node.xpath('./a/text()').getall(),
                     [item.replace('\r', '').replace('\n', '').replace('\t', '').strip()
                      for item in nace_node.xpath('./text()').getall() if
                      len(item.replace('\r', '').replace('\n', '').replace('\t', '').strip()) != 0])]  # I didn't
        # remember how we wanted these formatted but this creates a list of the nace & description | Doesn't have to
        # be one line, but it was fun. May not work depending on python version.
        reg = details.xpath('.//tr/td[contains(text(), \
        "Regulation")]/following-sibling::td/text()').get().replace('\r', '').replace(
            '\n', '').replace('\t', '').strip()  # pretty sure we break this out. Can look later or we can do in kettle
        dec_table = details.xpath('.//table[@id="decisions"]')  # pulling out to work with easier
        decision_1 = {'dec_date': '', 'dec_art': '', 'pub_date': '', 'pub_journ': '', 'text_date': '', 'dec_text': ''}
        decisions = []
        for index, row in enumerate(dec_table.xpath('./tr')[1:]):  # I don't know how I feel about this...
            if (index > 0 and row.xpath('./td[descendant::strong]')) or index == len(dec_table.xpath('./tr')[1:]):
                decisions.append(decision_1)
                decision_1 = {'dec_date': '', 'dec_art': '', 'pub_date': '', 'pub_journ': '', 'text_date': '',
                              'dec_text': ''}
            else:  # these might need to have all possible values but for ease I'm only adding what exists
                if row.xpath('./td[1]/strong/text()'):
                    decision_1['dec_date'] = row.xpath('./td[1]/strong/text()').get()
                if row.xpath('./td[2]/strong/text()'):
                    decision_1['dec_art'] = row.xpath('./td[2]/strong/text()').re(r"Art. ([\d\(\)\w]*)")
                if row.xpath('./td[contains(text(), "Publication")]/following-sibling::td/table//td/text()'):
                    decision_1['pub_date'] = row.xpath('./td[contains(text(), '
                                                       '"Publication")]/following-sibling::td/table//td/text()') \
                        .re(r"\d{2}\.\d{2}\.\d{4}")[0]
                    decision_1['pub_journ'] = row.xpath('./td[contains(text(), '
                                                        '"Publication")]/following-sibling::td/table//a/text()').get()
                if row.xpath('./td[contains(text(), "Press release")]/following-sibling::td/table//a/text()'):
                    decision_1['pr'] = row.xpath('./td[contains(text(), "Press release")]/'
                                                 'following-sibling::td/table//a/text()').get()
                if row.xpath('./td[contains(text(), "Decision text")]//following-sibling::td'):
                    decision_1['text_date'] = row.xpath('./td[contains(text(), "Decision text")]/following-sibling::'
                                                        'td//td[not(count(a))]/text()').get().replace('\r', '') \
                        .replace('\t', '').replace('\n', '').strip()
                    decision_1['dec_text'] = " ".join(row.xpath('./td[contains(text(), "Decision text")]/'
                                                                'following-sibling::td//a[not(count(img))]/text()').
                                                      getall())  # I just figured we might have multiples
            relation = details.xpath('//td[contains(text(), "Relation")]//following-sibling::td/text()').get(). \
                replace('\r', '').replace('\n', '').replace('\t', '').strip()
            other = " ".join([item.replace('\r', '').replace('\n', '').replace('\t', '').strip() for item in details.
                             xpath('./tr')[-2].xpath('.//text()').getall() if
                              len(item.replace('\r', '').replace('\n', '').replace('\t', '').strip()) != 0])  # neat.
            # The preceding td could be removed, but we can do that later
            related = " ".join([item.replace('\r', '').replace('\n', '').replace('\t', '').strip() for item
                                in details.xpath('./tr')[-1].xpath('.//text()').getall() if
                                len(item.replace('\r', '').replace('\n', '').replace('\t', '').strip()) != 0])  # same
            # with this we can clean up later but the selector should work (fingers crossed)

        for comp in comps:
            item['policy'] = response.meta['policy']
            item['case_number'] = response.meta['case_number']
            item['member_state'] = response.meta['member_state']
            item['last_decision_date'] = response.meta['last_decision_date']
            item['title'] = response.meta['title']
            item['company'] = comp
            item['notification_date'] = notif_date
            item['prov_deadline'] = prov_deadline
            item['prior_pub_journal'] = journal_no
            item['prior_pub_journal_date'] = journal_date
            item['naces'] = naces
            item['regulation'] = reg
            item['decisions'] = decisions
            item['relation'] = relation
            item['other_related'] = other
            item['rel_links'] = related
            yield item

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
