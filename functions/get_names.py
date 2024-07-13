import pandas as pd
import apache_beam as beam
import os
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

class GetNames(beam.DoFn):
    def process(self, element):
        logging.info(element)

        controler = pd.read_csv('data/controler.csv')
        
        yield list(controler['tables'])

