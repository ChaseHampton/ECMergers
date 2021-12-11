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
    pass
