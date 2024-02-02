from pyspark.sql.types import StructField, StringType, StructType, LongType, BooleanType, TimestampType

SCHEMA = StructType([
    StructField("tweet_id", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("user_id_str", StringType(), False),
    StructField("text", StringType(), False),
    StructField("hashtags", StringType(), True),
    StructField("retweet_count", LongType(), False),
    StructField("favorite_count", LongType(), False),
    StructField("in_reply_to_screen_name", StringType(), False),
    StructField("source", StringType(), False),
    StructField("retweeted", BooleanType(), False),
    StructField("lang", StringType(), False),
    StructField("location", StringType(), True),
    StructField("place_name", StringType(), True),
    StructField("place_lat", StringType(), True),
    StructField("place_lon", StringType(), True),
    StructField("screen_name", StringType(), False)
])

BIDEN_NAME = "JoeBiden"
TRUMP_NAME = "realDonaldTrump"

STATES = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
          'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
          'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
          'Nevada', 'New Hampshire', 'New Jersey', 'New York', 'New Mexico', 'North Carolina', 'North Dakota', 'Ohio',
          'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
          'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

STATE_CODES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
               'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND',
               'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

STATE_CODES_MAPPING = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
                       'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
                       'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
                       'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
                       'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
                       'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NY': 'New York',
                       'NM': 'New Mexico', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
                       'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
                       'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
                       'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}

ELECTORAL_VOTES = {
    'Alabama': 9,
    'Alaska': 3,
    'Arizona': 11,
    'Arkansas': 6,
    'California': 55,
    'Colorado': 9,
    'Connecticut': 7,
    'Delaware': 3,
    'Florida': 29,
    'Georgia': 16,
    'Hawaii': 4,
    'Idaho': 4,
    'Illinois': 20,
    'Indiana': 11,
    'Iowa': 6,
    'Kansas': 6,
    'Kentucky': 8,
    'Louisiana': 8,
    'Maine': 4,
    'Maryland': 10,
    'Massachusetts': 11,
    'Michigan': 16,
    'Minnesota': 10,
    'Mississippi': 6,
    'Missouri': 10,
    'Montana': 3,
    'Nebraska': 5,
    'Nevada': 6,
    'New Hampshire': 4,
    'New Jersey': 14,
    'New Mexico': 5,
    'New York': 29,
    'North Carolina': 15,
    'North Dakota': 3,
    'Ohio': 18,
    'Oklahoma': 7,
    'Oregon': 7,
    'Pennsylvania': 20,
    'Rhode Island': 4,
    'South Carolina': 9,
    'South Dakota': 3,
    'Tennessee': 11,
    'Texas': 38,
    'Utah': 6,
    'Vermont': 3,
    'Virginia': 13,
    'Washington': 12,
    'West Virginia': 5,
    'Wisconsin': 10,
    'Wyoming': 3,
    'District of Columbia': 3
}




