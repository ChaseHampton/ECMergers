# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field

class EcmergerItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    policy = Field()
    case_number = Field()
    member_state = Field()
    last_decision_date = Field()
    title = Field()
    company = Field()
    notification_date = Field()
    prov_deadline = Field()
    prior_pub_journal = Field()
    prior_pub_journal_date = Field()
    naces = Field()
    regulation = Field()
    decisions = Field()
    relation = Field()
    other_related = Field()
    rel_links = Field()
    pass
