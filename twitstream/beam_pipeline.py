'''An apache beam pipeline to read and analyze tweets about streaming'''

from __future__ import absolute_import

import argparse
import logging
import re

import os
from dotenv import load_dotenv

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam import window

from google.cloud import pubsub_v1

from pipeline_transforms import (WordExtractingDoFn, GroupWindowsIntoBatches, 
                                    AddTimestamps, WriteBatchesToGCS)

load_dotenv()

# Configure google pubsub publisher
project_name = os.getenv("PROJECT_NAME")
topic_name = os.getenv("PROJECT_TOPIC")
input_topic = f'projects/{project_name}/topics/{topic_name}'
pipeline_options = PipelineOptions(streaming=True, save_main_session=True)
output_path = os.getenv("OUTPUT_PATH")
temp_path = os.getenv("TEMP_PATH")
subscription = os.getenv("SUBSCRIPTION")
pubsub_options = pipeline_options.view_as(GoogleCloudOptions)
pubsub_options.project = project_name
pubsub_options.staging_location = output_path
pubsub_options.temp_location =temp_path
pubsub_options.region = os.getenv("REGION")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_name, topic_name)

with beam.Pipeline(options=pipeline_options) as pipeline:
        raw_text = (pipeline
                    | "Read PubSub Messages" 
                    >> beam.io.ReadFromPubSub(
                        # topic=input_topic,
                        subscription=subscription,
                        with_attributes=False))

        windows = ( raw_text | "Window into" >> GroupWindowsIntoBatches())
        output = windows | 'Write' >> WriteToText(known_args.output, shard_name_template = '') # Write results
        output = (windows | "Write to GCS" 
                    >> beam.ParDo(WriteBatchesToGCS(output_path)))

# with beam.Pipeline(options=pipeline_options) as pipeline:
    
#     lines = (pipeline | 'Read' >> ReadFromPubSub(topic = input_topic))
    
#     counts = (
#         lines
#         | 'Split' >>
#         (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode)) # Split into words
#         | 'PairWithOne' >> beam.Map(lambda x: (x, 1)) # Pairwise keys with word and count
#         | 'GroupAndSum' >> beam.CombinePerKey(sum)) # Aggregate

#     # print('counts: ', counts)
#     def format_result(word, count):
#         return f'{word}: {count}'

#     output = (
#         counts 
#         | 'Format' >> beam.MapTuple(format_result)) # Map result as a string

#     output | 'Write' >> WriteToText('../data/output/tweetwordcount.txt', shard_name_template='')
#     (
#         pipeline
#         | 'read input' >> ReadFromPubSub(topic = input_topic)
#         | 'PairWithKey' >> beam.Map(lambda x: (x, 1))
#         | 'GroupAndSum' >> beam.CombinePerKey(sum) # Aggregate
#         | 'write output' >> WriteToText('../data/output/tweets.txt', shard_name_template = '')
#     )

