import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import apache_beam.io.filesystems as fs
import apache_beam.transforms.window as window
import csv
from collections import Counter
import json
import logging
from datetime import datetime

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_topic', default='projects/mlrit2-460809/topics/fraud-detection')
        parser.add_argument('--output_table', default='mlrit2-460809:pub_sub_data1.p3_table')
        parser.add_argument('--local_mode', action='store_true')

class ReadCSVFile(beam.DoFn):
    def process(self, file_path):
        try:
            with fs.FileSystems.open(file_path) as f:
                reader = csv.DictReader((line.decode('utf-8') for line in f))
                for row in reader:
                    yield row
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            raise

class ExtractColsForAggregation(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema

    def process(self, row):
        try:
            for col, dtype in self.schema.items():
                val = row.get(col)
                if val is None or val == '':
                    continue
                try:
                    if dtype == 'int':
                        yield (col, int(val))
                    elif dtype == 'double':
                        yield (col, float(val))
                    else:
                        yield (col, val)
                except ValueError:
                    logging.warning(f"Skipping invalid value for {col}: {val}")
            yield beam.pvalue.TaggedOutput('rows', row)
        except Exception as e:
            logging.error(f"Failed to process row: {str(e)}")

class ComputeFillValues(beam.DoFn):
    def process(self, element):
        col, values = element
        values_list = list(values)
        if not values_list:
            return
        try:
            first_val = values_list[0]
            if isinstance(first_val, (int, float)):
                fill_val = sum(values_list) / len(values_list)
                if isinstance(first_val, int):
                    fill_val = int(round(fill_val))
            else:
                fill_val = Counter(values_list).most_common(1)[0][0]
            yield (col, fill_val)
        except Exception as e:
            logging.error(f"Error computing fill value for column {col}: {str(e)}")

class CleanRows(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema

    def process(self, row, fill_values):
        if not row.get('transaction_id'):
            logging.warning("Skipping row without transaction_id.")
            return
        cleaned_row = {}
        try:
            for col, dtype in self.schema.items():
                val = row.get(col)
                if val is None or val == '':
                    cleaned_row[col] = fill_values.get(col)
                else:
                    try:
                        if dtype == 'int':
                            cleaned_row[col] = int(val)
                        elif dtype == 'double':
                            cleaned_row[col] = float(val)
                        elif dtype == 'timestamp':
                            dt = datetime.strptime(val, '%d-%m-%Y %H:%M')
                            cleaned_row[col] = dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            cleaned_row[col] = val
                    except Exception as e:
                        logging.warning(f"Invalid {col}={val}, using fill value. Error: {str(e)}")
                        cleaned_row[col] = fill_values.get(col)
            yield cleaned_row
        except Exception as e:
            logging.error(f"Failed to clean row: {str(e)}")

def run():
    options = CustomOptions()
    standard_opts = options.view_as(StandardOptions)
    local_mode = options.local_mode

    if local_mode:
        standard_opts.runner = 'DirectRunner'
        standard_opts.streaming = False
    else:
        standard_opts.runner = 'DataflowRunner'
        standard_opts.streaming = True

    schema = {
        'transaction_id': 'string',
        'user_id': 'string',
        'transaction_amount': 'double',
        'transaction_type': 'string',
        'time_stamp': 'timestamp',
        'account_balance': 'double',
        'device_type': 'string',
        'location': 'string',
        'merchant_category': 'string',
        'ip_address_flag': 'int',
        'previous_fraudulent_activity': 'int',
        'daily_transaction_count': 'int',
        'avg_transaction_amount_7d': 'double',
        'failed_transaction_count_7d': 'int',
        'card_type': 'string',
        'card_age': 'int',
        'transaction_distance': 'double',
        'authentication_method': 'string',
        'risk_score': 'double',
        'is_weekend': 'int',
        'fraud_label': 'int'
    }

    bq_schema_str = (
        'transaction_id:STRING,user_id:STRING,transaction_amount:FLOAT,transaction_type:STRING,'
        'time_stamp:TIMESTAMP,account_balance:FLOAT,device_type:STRING,location:STRING,'
        'merchant_category:STRING,ip_address_flag:INTEGER,previous_fraudulent_activity:INTEGER,'
        'daily_transaction_count:INTEGER,avg_transaction_amount_7d:FLOAT,failed_transaction_count_7d:INTEGER,'
        'card_type:STRING,card_age:INTEGER,transaction_distance:FLOAT,authentication_method:STRING,'
        'risk_score:FLOAT,is_weekend:INTEGER,fraud_label:INTEGER'
    )

    with beam.Pipeline(options=options) as p:
        if local_mode:
            file_paths = p | "CreateMockInput" >> beam.Create([
                "gs://online-payments2/test/sample1.csv",
                "gs://online-payments2/test/sample2.csv"
            ])
        else:
            file_paths = (
                p
                | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
                | "DecodeJSON" >> beam.Map(lambda msg: json.loads(msg.decode('utf-8')))
                | "ExtractGCSPath" >> beam.Map(lambda x: f"gs://{x['bucket']}/{x['file']}")
            )

        rows = file_paths | "ReadCSVFiles" >> beam.ParDo(ReadCSVFile())

        extract_results = rows | "ExtractColumns" >> beam.ParDo(ExtractColsForAggregation(schema)).with_outputs('rows', main='cols')

        fill_values = (
            extract_results.cols
            | "WindowInto" >> beam.WindowInto(window.FixedWindows(60)) if not local_mode else beam.transforms.util.Identity()
            | "GroupByKey" >> beam.GroupByKey()
            | "ComputeFillValues" >> beam.ParDo(ComputeFillValues())
        )
        fill_values_dict = beam.pvalue.AsDict(fill_values)

        cleaned_rows = extract_results.rows | "CleanRows" >> beam.ParDo(CleanRows(schema), fill_values=fill_values_dict)

        cleaned_rows | "WriteToBigQuery" >> WriteToBigQuery(
            table=options.output_table,
            schema=bq_schema_str,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# python3 pipeline.py \
#   --project=mlrit2-460809 \
#   --region=asia-south1 \
#   --temp_location=gs://online-payments2/temp \
#   --runner=DataflowRunner
