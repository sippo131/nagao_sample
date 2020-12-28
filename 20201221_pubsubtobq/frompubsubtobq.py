import argparse
import logging
import json

from past.builtins import unicode

import apache_beam as beam

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class JSONStringToDictFn(beam.DoFn):
    def process(self, element):
        items = json.loads(element)

        yield items

class TransformValueFn(beam.DoFn):
  def process(self, element):
      element["message_content"] += "doggo!"

      return element

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input_topic',
      required=True,
      help=(
          'Input PubSub subscription '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
    parser.add_argument(
      '--output_dataset',
      required=True,
      help=(
          'Output BigQuery dataset '
          '"<PROJECT>.<DATASET>"'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        input_topic = known_args.input_topic
        output_dataset = known_args.output_dataset

        table_schema = {
            'fields': [{
                'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'message_content', 'type': 'STRING', 'mode': 'NULLABLE'
            }]
        }

        read_elements = (
            p
            | 'Read' >> ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | 'ToDict' >> beam.ParDo(JSONStringToDictFn())
        )

        # write un-transformed data to BigQuery
        read_elements | 'Write read elements to BigQuery' >> WriteToBigQuery(
                                                             output_dataset,
                                                             schema=table_schema
                                                             )

        transformed_elements = (
            read_elements
            | 'Transform' >> beam.ParDo(TransformValueFn())
        )

        # write transformed data to BigQuery
        transformed_elements | 'Write transformed elements to BigQuery' >> WriteToBigQuery(
                                                             output_dataset,
                                                             schema=table_schema
                                                             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


"""
commandlinescript:

python frompubsubtobq.py \
--project=ca-nagao-test-297401 \
--runner=DataflowRunner \
--autoscaling_algorithm=THROUGHPUT_BASED \
--max_num_workers=3 \
--input_topic=projects/ca-nagao-test-297401/topics/cron-topic \
--output_dataset=ca-nagao-test-297401:test_dataset.test_frompubsub \
--temp_location=gs://apachebeam_storage/temp \
--staging_location=gs://apachebeam_storage/staging

"""
