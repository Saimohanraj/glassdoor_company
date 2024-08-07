import re
import os
import json
import scrapy
import hashlib
from datetime import datetime
#from glassdoor_company.items import GlassDoorItem

class GlassDoorSpider(scrapy.Spider):
    name = "glassdoor_canada"
    custom_settings = {
        # 'ITEM_PIPELINES': {"glass_server.pipelines.GlassdoorScraperPipeline": 300,},
        'FEED_EXPORT_ENCODING' : "utf-8",
        # 'FEEDS': {f"s3://{os.getenv('OUTPUT_BUCKET')}/output/daily_collections/cushmanwakefield/{current_date}/{name}.json": {"format": "json"}},
    }

    
    headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'apollographql-client-name': 'company.explorer',
            'apollographql-client-version': '3.20.23',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'cookie': 'gdId=f2694984-c135-4821-a0d6-ff224f32c5af; company_data=%7B%22company_name%22%3A%22Cittabase%20Solutions%22%2C%22industry%22%3A%22Software%20and%20Technology%22%2C%22country%22%3A%22India%22%2C%22state%22%3A%22Tamil%20Nadu%22%2C%22employee_range%22%3A%2220%20-%2049%22%2C%22buying_stage%22%3Anull%7D; _ga=GA1.2.158596302.1719027039; _optionalConsent=true; rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX19egAYvlKkY9kI4q%2BjbfD0OcX7tOSo0ZHqg6fn9qxDP%2B7%2BUInLd75gTu9tVLF2XMdhQjd9rYUd61w%3D%3D; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX1%2BrtRsGM95Xfe9j0VJO0GxDLQMAPEGt4wQ9zuku1sveIuiGV8bLAobU; _gcl_au=1.1.161434299.1719027042; __pdst=fd4e4a81bc644a65bf4f6c07547ead18; _uetvid=cdc30f90304711ef8a972d6c5f5a116d; _gd_visitor=9f0c7660-3eb0-488c-8bf2-c9d1256cc2ec; _gd_svisitor=8fc333b83d570100c672d465d5030000a86e2102; _mkto_trk=id:899-LOT-464&token:_mch-glassdoor.com-1719027042659-36919; _pin_unauth=dWlkPU56UTFNell3TnpRdE1ETmlOQzAwTnprNUxUbG1NekV0TXpoak9UbG1PV0k1WkdZeg; _fbp=fb.1.1719027042871.967400822913163331; _tt_enable_cookie=1; _ttp=xCKFSzy1g5PwQTGGkQ46wvKU47a; _gid=GA1.2.644494158.1719209872; trs=https%3A%2F%2Fwww.reddit.com%2F:referral:referral:2024-06-24+01%3A34%3A04.583:undefined:undefined; rttdf=true; indeedCtk=1i17k0dsh2a45001; ki_r=; ki_s=237369%3A0.0.0.0.0; ki_t=1719218047740%3B1719320993929%3B1719322223285%3B2%3B15; _cfuvid=6XnC8JvoZVaN0tYMC0o_8Bw.mbXjV_cZX44Drkba2oM-1719384773293-0.0.1.1-604800000; _ga_500H17GZ8D=GS1.2.1719397950.1.1.1719398125.43.0.0; cf_clearance=DGcF_2hW19NjoqV.hA80vD9r9K6YKT14EgXDVHReZ70-1719398124-1.0.1.1-R5y7gSi9zSbpZxeoppX739pQL9qyj2u95CDFoM74zgdU5gpYIwDEcQrECzTvts9KtKD9zBmDliQWuOEwgG3fLQ; rl_user_id=RudderEncrypt%3AU2FsdGVkX1%2FRj%2F9gNm4jYsOycFq9p7d1Vbi9ejWKP2Y%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX1%2F9f61n%2FeJ%2F2KMg2P9DQuQY8idWM3WjHLU%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX18Iga2EvrbjdFG4aTXZ9%2B2q%2F012QtXGtQE%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX18WL%2B0ctTW9VNzB9pA%2F%2FfuxD4wKJripOcE%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX1%2F7a2MZn4E0VxV8fctYDtmENO%2FvDApWie0DVzTAAmtNCke7%2Bht%2Bgl3rWsTwSpf6u7Pv0Vpn5E69Pg%3D%3D; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Jun+26+2024+18%3A44%3A14+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=6ee3c77f-367a-428c-8dfd-ad25e25ba7d0&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0017%3A1&AwaitingReconsent=false; _rdt_uuid=1719027042421.b1e9f36d-0603-4465-90cf-df3b3b9c3a2e; asst=1719468988.0; bs=V9Ji9kSNeK57MNNAsUzF1A:pljwYyQIIOOVXvKq_e2BwbGvOza696sUckG5VBW6DLSPaFyouuusA91AZEsnvbw0Ijj6vuUgjVPTPS2qH7qCfb6T_U8ZXxOHhoelf2yF2Hk:8DbCBDZS4ISkCOmLEcCN0GScLGzKvyLjqUXivHFn7ks; __cf_bm=K3MSVObn7m0niBXlPXrAa1pCA6slf30lofH2b6pCA1E-1719468988-1.0.1.1-wdi8AtXYu39TSoRANBksf9Lul.JZvuH1num5sujTmAP7WQ7nmWdANGwDexl3poYy7T.Cmwmn3y8v1.ZTJ6fVZc1S17FyoKXQ0BIPdPX7cy4; _dc_gtm_UA-2595786-1=1; AWSALB=ki3oT9qRMiQVpRoIs7Ng0Hjd6I3Wq3lp9gmC7MBavbdnpmvLLqky3+Jas5o+VZqbGaantTv/RvN8dTJhOhMzOBRcT6S5TfnnT/i0Q2LJ6mqxedpmff6hgCJaahWr0ljDoQzjCVQh+43ORM5lbBhFoUYA2WONgHgf4j/iaaYfUqunefd6oXhjUZVWBDWSow==; AWSALBCORS=ki3oT9qRMiQVpRoIs7Ng0Hjd6I3Wq3lp9gmC7MBavbdnpmvLLqky3+Jas5o+VZqbGaantTv/RvN8dTJhOhMzOBRcT6S5TfnnT/i0Q2LJ6mqxedpmff6hgCJaahWr0ljDoQzjCVQh+43ORM5lbBhFoUYA2WONgHgf4j/iaaYfUqunefd6oXhjUZVWBDWSow==; JSESSIONID=6EA5A00338D9738A4C7E1EF7DDA1CFF7; GSESSIONID=f2694984-c135-4821-a0d6-ff224f32c5af+1719453147772; cass=1; gdsid=1719453147772:1719468997793:BFF45A4ADCB84F273751371208572374; cdArr=180%2C180%2C180%2C180; rl_session=RudderEncrypt%3AU2FsdGVkX1%2F62Fm4wFexffxDB1NddSx3IK63gAimJKKilAp2%2BZebjXg6AP80JybSeHYb28Cp547h0muSZMd4AHsr2OszzFHNQn8A25qHDajbfRFpt1tDVbGKoPa4xFxk5xz2EwXdtffEA6%2Fez%2BwyJg%3D%3D; _cfuvid=ReIQfoAuPSGcAnkmcMz4q8rzWLuBPkLhz_QnDH4XhIY-1719234860808-0.0.1.1-604800000; AWSALB=dC+1sJ5W1n77z8UhMaolJVkOie4IbYyFVmqaF6zevmbWh59bK6iLid4clR4jJwjKmg1IUISCRhkIampv8k/8Prz9ckJ4hXT5OhI9dq3BHk0vot4ilYihBUAbTWw48h2MbSnCPApTlaot7hZG0wElFTzbAPP0SgUvAZk4nJAJY91TvjBKaOX35emygB1yCw==; AWSALBCORS=dC+1sJ5W1n77z8UhMaolJVkOie4IbYyFVmqaF6zevmbWh59bK6iLid4clR4jJwjKmg1IUISCRhkIampv8k/8Prz9ckJ4hXT5OhI9dq3BHk0vot4ilYihBUAbTWw48h2MbSnCPApTlaot7hZG0wElFTzbAPP0SgUvAZk4nJAJY91TvjBKaOX35emygB1yCw==; asst=1719468988.0; gdId=f2694984-c135-4821-a0d6-ff224f32c5af',
            'gd-csrf-token': '75UhZw5vn2Q3mcAHYgX33g:I6RJqtrWpTwFL20fYLZeSFbSpyrr43hHlSfHqmtN_opUCFmW1A8kf1tcnGn_NEsd3j_I8bWXT26pbaev3g_0Xw:6u3AmL7p5OGdaPi7qB_W-XO9hVJW8tpW9CQMF3IXvqw',
            'origin': 'https://www.glassdoor.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.glassdoor.com/',
            'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"124.0.6367.91"',
            'sec-ch-ua-full-version-list': '"Chromium";v="124.0.6367.91", "Google Chrome";v="124.0.6367.91", "Not-A.Brand";v="99.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Linux"',
            'sec-ch-ua-platform-version': '"6.5.0"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            }

    def regex(self, word):
        word = re.sub(r'\s+', ' ', word)
        word = word.replace('\n', ' ')

        return word
    def start_requests(self):
        countries = [{'name': 'Canda', 'id': 3}]
        categories = [{"id": 200001, "value": "Accounting & Tax"},{"id": 200022, "value": "Advertising & Public Relations"},{"id": 10002, "value": "Aerospace & Defense"},{"id": 10003, "value": "Agriculture"},{"id": 200134, "value": "Airlines, Airports & Air Transportation"},{"id": 200149, "value": "Ambulance & Medi     Transportation"},{"id": 200004, "value": "Animal Production"},{"id": 200023, "value": "Architectural & Engineering Services"},{"id": 10004, "value": "Arts, Entertainment & Recreation"},{"id": 200100, "value": "Auctions & Galleries"},{"id": 200011, "value": "Audiovisual"},{"id": 200101, "value": "Automotive Parts & Accessories Stores"},{"id": 200048, "value": "Banking & Lending"},{"id": 200012, "value": "Bars & Nightclubs"},{"id": 200102, "value": "Beauty & Personal Accessories Stores"},{"id": 200040, "value": "Beauty & Wellness"},{"id": 200021, "value": "Biotech & Pharmaceuticals"},{"id": 200163, "value": "Biotechnology"},{"id": 200082, "value": "Broadcast Media"},{"id": 200024, "value": "Building & Personnel Services"},{"id": 200028, "value": "Business Consulting"},{"id": 200120, "value": "Cable, Internet & Telephone Providers"},{"id": 200132, "value": "Car & Truck Rental"},{"id": 200096, "value": "Catering & Food Service Contractors"},{"id": 200068, "value": "Chemical Manufacturing"},{"id": 200089, "value": "Civic & Social Services"},{"id": 200044, "value": "Colleges & Universities"},{"id": 200035, "value": "Commercial Equipment Services"},{"id": 200027, "value": "Commercial Printing"},{"id": 200060, "value": "Computer Hardware Development"},{"id": 200036, "value": "Construction"},{"id": 10007, "value": "Construction, Repair & Maintenance Services"},{"id": 200103, "value": "Consumer Electronics & Appliances Stores"},{"id": 200147, "value": "Consumer Product Manufacturing"},{"id": 200038, "value": "Consumer Product Rental"},{"id": 200097, "value": "Convenience Stores"},{"id": 200008, "value": "Crop Production"},{"id": 200016, "value": "Culture & Entertainment"},{"id": 200148, "value": "Debt Relief"},{"id": 200150, "value": "Dental Clinics"},{"id": 200105, "value": "Department, Clothing & Shoe Stores"},{"id": 200106, "value": "Drug & Health Stores"},{"id": 10009, "value": "Education"},{"id": 200045, "value": "Education & Training Services"},{"id": 200070, "value": "Electronics Manufacturing"},{"id": 200091, "value": "Energy & Utilities"},{"id": 10019, "value": "Energy, Mining & Utilities"},{"id": 200061, "value": "Enterprise Software & Network Solutions"},{"id": 200039, "value": "Event Services"},{"id": 200006, "value": "Farm Support"},{"id": 200077, "value": "Film Production"},{"id": 10010, "value": "Financial Services"},{"id": 200052, "value": "Financial Transaction Processing"},{"id": 200005, "value": "Fishery"},{"id": 200007, "value": "Floral Nursery"},{"id": 200071, "value": "Food & Beverage Manufacturing"},{"id": 200107, "value": "Food & Beverage Stores"},{"id": 200009, "value": "Forestry, Logging & Timber Operations"},{"id": 200013, "value": "Gambling"},{"id": 200109, "value": "General Merchandise & Superstores"},{"id": 200037, "value": "General Repair & Maintenance"},{"id": 200110, "value": "Gift, Novelty & Souvenir Stores"},{"id": 10011, "value": "Government & Public Administration"},{"id": 200087, "value": "Grantmaking & Charitable Foundations"},{"id": 200145, "value": "Grocery Stores"},{"id": 200072, "value": "Health Care Products Manufacturing"},{"id": 200059, "value": "Health Care Services & Hospitals"},{"id": 10012, "value": "Healthcare"},{"id": 200111, "value": "Home Furniture & Housewares Stores"},{"id": 200151, "value": "Hospitals & Health Clinics"},{"id": 200139, "value": "Hotels & Resorts"},{"id": 10025, "value": "Hotels & Travel Accommodation"},{"id": 200032, "value": "HR Consulting"},{"id": 10026, "value": "Human Resources & Staffing"},{"id": 10013, "value": "Information Technology"},{"id": 200064, "value": "Information Technology Support Services"},{"id": 10014, "value": "Insurance"},{"id": 200065, "value": "Insurance Agencies & Brokerages"},{"id": 200066, "value": "Insurance Carriers"},{"id": 200063, "value": "Internet & Web Services"},{"id": 200146, "value": "Investment & Asset Management"},{"id": 200041, "value": "Laundry & Dry Cleaning"},{"id": 200156, "value": "Law Firms"},{"id": 10001, "value": "Legal"},{"id": 200157, "value": "Legal Services"},{"id": 200073, "value": "Machinery Manufacturing"},{"id": 10006, "value": "Management & Consulting"},{"id": 10015, "value": "Manufacturing"},{"id": 200166, "value": "Marine Transportation"},{"id": 10016, "value": "Media & Communication"},{"id": 200113, "value": "Media & Entertainment Stores"},{"id": 200152, "value": "Medical Testing & Clinical Laboratories"},{"id": 200029, "value": "Membership Organizations"},{"id": 200074, "value": "Metal & Mineral Manufacturing"},{"id": 200085, "value": "Mining & Metals"},{"id": 200057, "value": "Municipal Agencies"},{"id": 200160, "value": "Music & Sound Production"},{"id": 200056, "value": "National Agencies"},{"id": 10018, "value": "Nonprofit & NGO"},{"id": 200153, "value": "Nursing Care Facilities"},{"id": 200025, "value": "Office Supply & Copy Stores"},{"id": 200115, "value": "Other Retail Stores"},{"id": 200127, "value": "Parking & Valet"},{"id": 10008, "value": "Personal Consumer Services"},{"id": 200116, "value": "Pet & Pet Supplies Stores"},{"id": 200043, "value": "Pet Care & Veterinary"},{"id": 200164, "value": "Pharmaceutical"},{"id": 10005, "value": "Pharmaceutical & Biotechnology"},{"id": 200017, "value": "Photography"},{"id": 200047, "value": "Preschools & Child Care Services"},{"id": 200046, "value": "Primary & Secondary Schools"},{"id": 200162, "value": "Private Households"},{"id": 200042, "value": "Property Management"},{"id": 200080, "value": "Publishing"},{"id": 200128, "value": "Rail Transportation"},{"id": 10020, "value": "Real Estate"},{"id": 200165, "value": "Real Estate Agencies"},{"id": 200088, "value": "Religious Institutions"},{"id": 200030, "value": "Research & Development"},{"id": 200099, "value": "Restaurants & Cafes"},{"id": 10021, "value": "Restaurants & Food Service"},{"id": 10022, "value": "Retail & Wholesale"},{"id": 200031, "value": "Security & Protective"},{"id": 200130, "value": "Shipping & Trucking"},{"id": 200155, "value": "Software Development"},{"id": 200117, "value": "Sporting Goods Stores"},{"id": 200018, "value": "Sports & Recreation"},{"id": 200154, "value": "Staffing & Subcontracting"},{"id": 200058, "value": "State & Regional Agencies"},{"id": 200055, "value": "Stock Exchanges"},{"id": 200135, "value": "Taxi & Car Services"},{"id": 10023, "value": "Telecommunications"},{"id": 200122, "value": "Telecommunications Services"},{"id": 200159, "value": "Textile & Apparel Manufacturing"},{"id": 200020, "value": "Ticket Sales"},{"id": 200118, "value": "Toy & Hobby Stores"},{"id": 200161, "value": "Translation & Linguistic Services"},{"id": 10024, "value": "Transportation & Logistics"},{"id": 200075, "value": "Transportation Equipment Manufacturing"},{"id": 200144, "value": "Travel Agencies"},{"id": 200119, "value": "Vehicle Dealers"},{"id": 200034, "value": "Vehicle Repair & Maintenance"},{"id": 200083, "value": "Video Game Publishing"},{"id": 200158, "value": "Waste Management"},{"id": 200033, "value": "Wholesale"},{"id": 200076, "value": "Wood & Paper Manufacturing"},]
        for country_loop in countries:
            country = country_loop['id']
            for cate in categories:
                id_categories = cate['id']
                industry = cate['value']
                rate_max = 'rating_max'
                rate_min= 'rating_min'
                url = "https://www.glassdoor.com/graph"

                payload = json.dumps([
                        {
                            "operationName": "ExplorerEmployerSearchGraphQuery",
                            "variables": {
                            "employerSearchRangeFilters": [
                                {
                                "filterType": "RATING_OVERALL",
                                "maxInclusive": 5,
                                "minInclusive": 0
                                }
                            ],
                            "industries": [
                                {
                                "id": id_categories
                                }
                            ],
                            "jobTitle": "",
                            "location": {
                                "locationId": country,
                                "locationType": "N"
                            },
                            "pageRequested": 1,
                            "preferredTldId": 1,
                            "sGocIds": [],
                            "sectors": []
                            },
                            "query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"
                        }
                        ])
                yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_employers,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country},dont_filter=True)
    def parse_employers(self, response,id_categories,industry,rate_max,rate_min,country):
        json_data = json.loads(response.text)
        number_of_records = json_data[0]['data']['employerSearchV2']['numOfRecordsAvailable']
        if number_of_records !=0:
            if number_of_records<10000:
                numOfPagesAvailable = json_data[0]['data']['employerSearchV2']['numOfPagesAvailable']
                for pagination in range(1, int(numOfPagesAvailable)+1):
                    url = "https://www.glassdoor.com/graph"
                    payload = json.dumps([
                    {"operationName": "ExplorerEmployerSearchGraphQuery","variables": {"employerSearchRangeFilters": [{"filterType": "RATING_OVERALL","maxInclusive": 5,"minInclusive": 0}],
                        "industries": [
                                {
                                "id": id_categories
                                }
                            ],
                        "jobTitle": "",
                        "location": {
                            "locationId": country,
                            "locationType": "N"
                        },
                        "pageRequested": pagination,
                        "preferredTldId": 1,
                        "sGocIds": [],
                        "sectors": []
                        },
                        "query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"
                    }
                    ])
                    yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_detail_collection,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})
            else:
                rating_ranges = [{"min": 1, "max": 50}, {"min": 51, "max": 200}, {"min": 201, "max": 500}, {"min": 501, "max": 1000}, {"min": 1001, "max": 5000}, {"min": 5001, "max": 10000},{'min':10001,"max":9007199254740991 }]
                for rate in rating_ranges:
                    rate_max = rate['max']
                    rate_min= rate['min']
                    url = "https://www.glassdoor.com/graph"
                    payload = json.dumps([{"operationName": "ExplorerEmployerSearchGraphQuery","variables": {"employerSearchRangeFilters": [{"filterType": "RATING_OVERALL","maxInclusive": 5,"minInclusive": 0},{"filterType": "EMPLOYEES_COUNT","maxInclusive": rate_max,"minInclusive": rate_min}],"industries": [{"id": id_categories}],"jobTitle": "","location": {"locationId": country,"locationType": "N"},"pageRequested": 1,"preferredTldId": 1,"sGocIds": [],"sectors": []},"query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"}])
                    yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_industry_pagination,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})           
        else:
            url = "https://www.glassdoor.com/graph"
            payload = json.dumps([
            {
                "operationName": "ExplorerEmployerSearchGraphQuery",
                "variables": {
                "employerSearchRangeFilters": [
                    {
                    "filterType": "RATING_OVERALL",
                    "maxInclusive": 5,
                    "minInclusive": 0
                    }
                ],
                "industries": [
                ],
                "jobTitle": "",
                "location": {
                    "locationId": country,
                    "locationType": "N"
                },
                "pageRequested": 1,
                "preferredTldId": 1,
                "sGocIds": [],
                "sectors": [
                    {
                    "id": id_categories
                    }]
                },
                "query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"
            }
            ])
            yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_else_part,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})
    def parse_industry_pagination(self, response,id_categories,industry,rate_max,rate_min,country):
        json_data = json.loads(response.text)
        number_of_records = json_data[0]['data']['employerSearchV2']['numOfRecordsAvailable']
        numOfPagesAvailable = json_data[0]['data']['employerSearchV2']['numOfPagesAvailable']
        for pagination in range(1, int(numOfPagesAvailable)+1):
            url = "https://www.glassdoor.com/graph"
            payload = json.dumps([{"operationName": "ExplorerEmployerSearchGraphQuery","variables": {"employerSearchRangeFilters": [{"filterType": "RATING_OVERALL","maxInclusive": 5,"minInclusive": 0},{"filterType": "EMPLOYEES_COUNT","maxInclusive": rate_max,"minInclusive": rate_min}],"industries": [{"id": id_categories}],"jobTitle": "","location": {"locationId": country,"locationType": "N"},"pageRequested": pagination,"preferredTldId": 1,"sGocIds": [],"sectors": []},"query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"}])
            yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_detail_collection,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})

    def parse_else_part(self, response,id_categories,industry,rate_max,rate_min,country):
        json_data = json.loads(response.text)
        number_of_records = json_data[0]['data']['employerSearchV2']['numOfRecordsAvailable']
        numOfPagesAvailable = json_data[0]['data']['employerSearchV2']['numOfPagesAvailable']
        if number_of_records <10000:
            for pagination in range(1, int(numOfPagesAvailable)+1):
                url = "https://www.glassdoor.com/graph"
                payload = json.dumps([
                {
                    "operationName": "ExplorerEmployerSearchGraphQuery",
                    "variables": {
                    "employerSearchRangeFilters": [
                        {
                        "filterType": "RATING_OVERALL",
                        "maxInclusive": 5,
                        "minInclusive": 0
                        }
                    ],
                    "industries": [
                    ],
                    "jobTitle": "",
                    "location": {
                        "locationId": country,
                        "locationType": "N"
                    },
                    "pageRequested": pagination,
                    "preferredTldId": 1,
                    "sGocIds": [],
                    "sectors": [
                        {
                        "id": id_categories
                        }]
                    },
                    "query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"
                }
                ])
                yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_detail_collection,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})
        else:
            rating_ranges = [{"min": 1, "max": 50}, {"min": 51, "max": 200}, {"min": 201, "max": 500}, {"min": 501, "max": 1000}, {"min": 1001, "max": 5000}, {"min": 5001, "max": 10000},{'min':10001,"max":9007199254740991 }]
            for rate in rating_ranges:
                rate_max = rate['max']
                rate_min= rate['min']
                url = "https://www.glassdoor.com/graph"
                payload = json.dumps([{"operationName": "ExplorerEmployerSearchGraphQuery","variables": {"employerSearchRangeFilters": [{"filterType": "RATING_OVERALL","maxInclusive": 5,"minInclusive": 0},{"filterType": "EMPLOYEES_COUNT","maxInclusive": rate_max,"minInclusive": rate_min}],"industries": [],"jobTitle": "","location": {"locationId": country,"locationType": "N"},"pageRequested": 1,"preferredTldId": 1,"sGocIds": [],"sectors": [{"id": id_categories}]},"query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"}])
                yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_sector_filter,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})           
    def parse_sector_filter(self, response,id_categories,industry,rate_max,rate_min,country):
        json_data = json.loads(response.text)
        number_of_records = json_data[0]['data']['employerSearchV2']['numOfRecordsAvailable']
        numOfPagesAvailable = json_data[0]['data']['employerSearchV2']['numOfPagesAvailable']
        for pagination in range(1, int(numOfPagesAvailable)+1):
            url = "https://www.glassdoor.com/graph"
            payload = json.dumps([{"operationName": "ExplorerEmployerSearchGraphQuery","variables": {"employerSearchRangeFilters": [{"filterType": "RATING_OVERALL","maxInclusive": 5,"minInclusive": 0},{"filterType": "EMPLOYEES_COUNT","maxInclusive": rate_max,"minInclusive": rate_min}],"industries": [],"jobTitle": "","location": {"locationId": country,"locationType": "N"},"pageRequested": pagination,"preferredTldId": 1,"sGocIds": [],"sectors": [{"id": id_categories}]},"query": "query ExplorerEmployerSearchGraphQuery($employerSearchRangeFilters: [EmployerSearchRangeFilter], $industries: [IndustryIdent], $jobTitle: String, $location: UgcSearchV2LocationIdent, $pageRequested: Int, $preferredTldId: Int, $sGocIds: [Int], $sectors: [SectorIdent]) {\n  employerSearchV2(\n    employerSearchRangeFilters: $employerSearchRangeFilters\n    industries: $industries\n    jobTitle: $jobTitle\n    location: $location\n    pageRequested: $pageRequested\n    preferredTldId: $preferredTldId\n    sGocIds: $sGocIds\n    sectors: $sectors\n  ) {\n    employerResults {\n      demographicRatings {\n        category\n        categoryRatings {\n          categoryValue\n          ratings {\n            overallRating\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      employer {\n        bestProfile {\n          id\n          __typename\n        }\n        id\n        shortName\n        ratings {\n          overallRating\n          careerOpportunitiesRating\n          compensationAndBenefitsRating\n          cultureAndValuesRating\n          diversityAndInclusionRating\n          seniorManagementRating\n          workLifeBalanceRating\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    numOfPagesAvailable\n    numOfRecordsAvailable\n    __typename\n  }\n}\n"}])
            yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_detail_collection,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})
    
    def parse_detail_collection(self,response,id_categories,industry,rate_max,rate_min,country):
        json_data = json.loads(response.text)
        employer_results = json_data[0]['data']['employerSearchV2']['employerResults']
        for employer in employer_results:
            employer_id = employer['employer']['id']
            employer_shortname = employer['employer']['shortName']
            employer_profileid = employer['employer']['bestProfile']['id']
            url = "https://www.glassdoor.com/graph"
            payload = json.dumps([
            {
                "operationName": "ExplorerEmployerResultsGraphQuery",
                "variables": {
                "employerProfileId": employer_profileid,
                "id": employer_id,
                "shortName": employer_shortname,
                "locationId": country,
                "locationType": "N"
                },
                "query": "query ExplorerEmployerResultsGraphQuery($employerProfileId: Int, $id: Int!, $locationId: Int, $locationType: String, $shortName: String) {\n  EmployerLocations: employerOfficeLocation(\n    employer: {id: $id, name: $shortName}\n    locationId: $locationId\n    locationType: $locationType\n  ) {\n    eiOfficesLocationUrl\n    officeAddresses {\n      addressLine1\n      addressLine2\n      administrativeAreaName1\n      cityName\n      id\n      officeLocationId\n      __typename\n    }\n    __typename\n  }\n  EmployerReviews: employerReviewsRG(\n    employerReviewsInput: {employer: {id: $id}, dynamicProfileId: $employerProfileId}\n  ) {\n    allReviewsCount\n    employer {\n      headquarters\n      id\n      counts {\n        globalJobCount {\n          jobCount\n          __typename\n        }\n        __typename\n      }\n      links {\n        overviewUrl\n        reviewsUrl\n        salariesUrl\n        __typename\n      }\n      overview {\n        description\n        __typename\n      }\n      primaryIndustry {\n        industryId\n        __typename\n      }\n      shortName\n      sizeCategory\n      squareLogoUrl\n      __typename\n    }\n    ratings {\n      overallRating\n      careerOpportunitiesRating\n      compensationAndBenefitsRating\n      cultureAndValuesRating\n      diversityAndInclusionRating\n      seniorManagementRating\n      workLifeBalanceRating\n      __typename\n    }\n    __typename\n  }\n  EmployerSalary: aggregatedSalaryEstimates(\n    aggregatedSalaryEstimatesInput: {employer: {id: $id}, location: {}}\n  ) {\n    salaryCount\n    __typename\n  }\n}\n"
            }
            ])
            yield scrapy.Request(url, method="POST", body=payload, headers=self.headers, callback=self.parse_employer_details,dont_filter=True,cb_kwargs={'industry':industry,'id_categories':id_categories,'rate_max':rate_max,'rate_min':rate_min,'country':country})
    def parse_employer_details(self, response,industry,id_categories,rate_max,rate_min,country):
        employer_detail = json.loads(response.text)
        employ = employer_detail[0]['data']
        item = {}
        item['industry'] = industry
        item['id_categories'] = id_categories
        item['country_id'] = country
        item['company_url'] = 'https://www.glassdoor.com'+employ['EmployerReviews']['employer']['links']['overviewUrl']
        item['company_name'] = str(employ['EmployerReviews']['employer']['shortName'])
        item['company_size_category'] = employ['EmployerReviews']['employer']['sizeCategory']
        item['company_id'] = employ['EmployerReviews']['employer']['id']
        item['company_headquarters'] = employ['EmployerReviews']['employer']['headquarters']
        item['location_url'] = ''
        item['company_description'] = ''
        company_description = employ['EmployerReviews']['employer']['overview']['description']
        if company_description:
            item['company_description'] = self.regex(company_description).strip()
        item['company_review_url'] = 'https://www.glassdoor.com'+employ['EmployerReviews']['employer']['links']['reviewsUrl']
        item['company_salary_url'] = 'https://www.glassdoor.com'+employ['EmployerReviews']['employer']['links']['salariesUrl']
        item['company_career_opportunities_rating'] = employ['EmployerReviews']['ratings']['careerOpportunitiesRating']
        item['company_compensation_and_benefits_rating'] = employ['EmployerReviews']['ratings']['compensationAndBenefitsRating']
        item['company_culture_and_values_rating'] = employ['EmployerReviews']['ratings']['cultureAndValuesRating']
        item['company_diversity_and_inclusion_rating'] = employ['EmployerReviews']['ratings']['diversityAndInclusionRating']
        item['company_overall_rating'] = employ['EmployerReviews']['ratings']['overallRating']
        item['company_senior_management_rating'] = employ['EmployerReviews']['ratings']['seniorManagementRating']
        item['company_work_life_balance_rating'] = employ['EmployerReviews']['ratings']['workLifeBalanceRating']
        item['all_reviews_count'] = employ['EmployerReviews']['allReviewsCount']
        item['salary_count'] = employ['EmployerSalary']['salaryCount']
        location_url = employ['EmployerLocations']['eiOfficesLocationUrl']
        if location_url:
            item['location_url'] = response.urljoin(location_url)
        all_values = [str(item[key]) for key in item if key != 'images']
        hash_obj = hashlib.md5(('_'.join(all_values)).encode('utf-8'))
        hash = hash_obj.hexdigest()
        item['hash'] = hash 
        yield item   
        
