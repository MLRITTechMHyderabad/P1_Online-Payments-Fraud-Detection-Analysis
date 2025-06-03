import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from datetime import datetime

class CleanAndTransformData(beam.DoFn):
    def process(self, element):
        try:
            # Parse timestamp with correct format
            dt = datetime.strptime(element['time_stamp'], "%Y-%m-%d %H:%M:%S")
            element['time_stamp'] = dt.isoformat()

            element['transaction_amount'] = float(element['transaction_amount'])
            element['account_balance'] = float(element['account_balance'])
            element['ip_address_flag'] = int(element['ip_address_flag'])
            element['previous_fraudulent_activity'] = int(element['previous_fraudulent_activity'])
            element['daily_transaction_count'] = int(element['daily_transaction_count'])
            element['avg_transaction_amount_7d'] = float(element['avg_transaction_amount_7d'])
            element['failed_transaction_count_7d'] = int(element['failed_transaction_count_7d'])
            element['card_age'] = int(element['card_age'])
            element['transaction_distance'] = float(element['transaction_distance'])
            element['risk_score'] = float(element['risk_score'])
            element['is_weekend'] = int(element['is_weekend'])
            element['fraud_label'] = int(element['fraud_label'])

            yield element
        except Exception as e:
            print(f"Error processing element {element}: {e}")
            # skip invalid records
            pass

def run():
    options = PipelineOptions(
        streaming=False,  
        project='mlrit2-460809',
        region='asia-south1',
        temp_location='gs://online-payments2/temp'
    )

    fieldnames = [
        'transaction_id', 'user_id', 'transaction_amount', 'transaction_type',
        'time_stamp', 'account_balance', 'device_type', 'location', 'merchant_category',
        'ip_address_flag', 'previous_fraudulent_activity', 'daily_transaction_count',
        'avg_transaction_amount_7d', 'failed_transaction_count_7d', 'card_type', 'card_age',
        'transaction_distance', 'authentication_method', 'risk_score', 'is_weekend', 'fraud_label'
    ]

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from Local CSV File' >> beam.io.ReadFromText('path/to/sample_data.csv', skip_header_lines=1)
            | 'Parse CSV line to dict' >> beam.Map(lambda line: dict(zip(fieldnames, next(csv.reader([line])))))
            | 'Clean and Transform Data' >> beam.ParDo(CleanAndTransformData())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='mlrit2-460809:pub_sub_data1.beam_table',
                schema=(
                    'transaction_id:STRING,user_id:STRING,transaction_amount:FLOAT,transaction_type:STRING,'
                    'time_stamp:TIMESTAMP,account_balance:FLOAT,device_type:STRING,location:STRING,merchant_category:STRING,'
                    'ip_address_flag:INTEGER,previous_fraudulent_activity:INTEGER,daily_transaction_count:INTEGER,'
                    'avg_transaction_amount_7d:FLOAT,failed_transaction_count_7d:INTEGER,card_type:STRING,card_age:INTEGER,'
                    'transaction_distance:FLOAT,authentication_method:STRING,risk_score:FLOAT,is_weekend:INTEGER,fraud_label:INTEGER'
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location='gs://online-payments2/temp'
            )
        )

if _name_ == '_main_':
    run()