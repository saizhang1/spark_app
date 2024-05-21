import unittest
from pyspark.sql import SparkSession
from spark_app import create_week_and_year_columns, filter_registered_events, filter_app_load_events, ParseMode, StaticMode
from glob import glob

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Unitest functions").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestFilterEvents(PySparkTestCase):
    dummy_data = [
            {"event":"registered", "timestamp":"2020-11-02T06:21:14.000Z", "initiator_id":1, "channel":"browser"},
            {"event":"app_loaded", "timestamp":"2020-11-03T06:24:42.000Z", "initiator_id":1, "device_type":"desktop"},
            {"event":"app_loaded", "timestamp":"2020-11-03T06:25:42.000Z", "initiator_id":2, "device_type":"desktop"}
        ]
    def test_registered_filter(self):
        df = self.spark.createDataFrame(self.dummy_data)
        result = filter_registered_events(df)
        self.assertEqual(result.count(), 1)
        self.assertTrue('time' in result.collect()[0])

    def test_app_load_filter(self):
        df = self.spark.createDataFrame(self.dummy_data)
        result = filter_app_load_events(df)
        self.assertEqual(result.count(), 2)
        self.assertTrue('time' in result.collect()[0])

class TestCreateWeekYearColumns(PySparkTestCase):
    def test_simple_week_year(self):
        df = self.spark.createDataFrame([
            {"timestamp":"2020-11-02T06:21:14.000Z"},
            {"timestamp":"2020-12-24T07:00:14.000Z"},
        ])
        result = create_week_and_year_columns(df, 'timestamp')

        self.assertEqual(result.collect()[0]['year'], 2020)
        self.assertEqual(result.collect()[0]['week_of_year'], 45)

        self.assertEqual(result.collect()[1]['year'], 2020)
        self.assertEqual(result.collect()[1]['week_of_year'], 52)

    def test_with_add_days(self):
        df = self.spark.createDataFrame([
            {"timestamp":"2020-11-02T06:21:14.000Z"}
        ])
        result = create_week_and_year_columns(df, 'timestamp', 7)

        self.assertEqual(result.collect()[0]['year'], 2020)
        self.assertEqual(result.collect()[0]['week_of_year'], 46)


class TestEndToEnd(PySparkTestCase):
    def test_happy_flow(self):
        outputDir = "/tmp/unittests"
        ParseMode(self.spark, "./test_data", outputDir)

        # Not good, this depends to much on the iner workings of our StaticMode/ParseMode methods
        files = glob(outputDir + '/*/*.parquet')
        self.assertTrue(len(files) > 0)

        percentage = StaticMode(self.spark, outputDir)
        self.assertEqual(round(percentage * 100), 33)


unittest.main(argv=[''], verbosity=0, exit=False)