# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GlassDoorItem(scrapy.Item):
    industry = scrapy.Field()
    id_categories = scrapy.Field()
    country_id = scrapy.Field()
    company_url = scrapy.Field()
    company_name = scrapy.Field()
    company_size_category = scrapy.Field()
    company_id = scrapy.Field()
    company_headquarters = scrapy.Field()
    company_description = scrapy.Field()
    company_review_url = scrapy.Field()
    company_salary_url = scrapy.Field()
    location_url = scrapy.Field()
    company_career_opportunities_rating= scrapy.Field()
    company_compensation_and_benefits_rating = scrapy.Field()
    company_culture_and_values_rating = scrapy.Field()
    company_diversity_and_inclusion_rating = scrapy.Field()
    company_overall_rating = scrapy.Field()
    company_senior_management_rating = scrapy.Field()
    company_work_life_balance_rating = scrapy.Field()
    all_reviews_count = scrapy.Field()
    salary_count = scrapy.Field()
    hash = scrapy.Field()