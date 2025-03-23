import os

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
TRUSTED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'trusted')
REFINED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'refined')

# Data file names
OFFERS_FILE = 'offers.csv'
CUSTOMERS_FILE = 'customers.csv'
EVENTS_FILE = 'events.csv'

# Spark configuration
SPARK_CONFIG = {
    'spark.app.name': 'Cafe Rewards Pipeline',
    'spark.executor.memory': '4g',
    'spark.driver.memory': '2g',
    'spark.executor.cores': '2',
    'spark.sql.shuffle.partitions': '20'
}
