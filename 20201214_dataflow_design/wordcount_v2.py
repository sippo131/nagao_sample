#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re
import datetime

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


"""
# WaitDoFn_v1:simple_subprocess
class WaitDoFn(beam.DoFn):

  def process(self, element):

    [i1, i2] = element.strip().split(",")
    logging.info("[%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    beam.utils.processes.call(["sleep", i2])

    logging.info("[%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    # DoFnの場合、配列で返す必要がある
    return [element]
"""

"""
# WaitDoFn_v2:complex_subprocess
class WaitDoFn(beam.DoFn):

  def process(self, element):

    [i1, i2] = element.strip().split(",")
    logging.info("[%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    beam.utils.processes.call(["sleep", i2])

    logging.info("[%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    # 2回目の場合だとわかるように記載
    return ["%s--,%s" % (i1, i2)]
"""

# WaitDoFn_v3:increase_data
class WaitDoFn(beam.DoFn):

  def process(self, element):

    [i1, i2] = element.strip().split(",")
    logging.info("[aokad][%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    beam.utils.processes.call(["sleep", i2])

    logging.info("[aokad][%s,%s] %s" % (i1, i2, datetime.datetime.now()))

    # increase the number of data
    # return ["%s--,%s" % (i1, i2)]
    return ["%s.1--,%s" % (i1, i2),
            "%s.2--,%s" % (i1, i2),
            "%s.3--,%s" % (i1, i2),
            "%s.4--,%s" % (i1, i2),
            "%s.5--,%s" % (i1, i2)]

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

      """
      # PATTERN_v1:simple_subprocess
      lines = p | 'Read' >> ReadFromText(known_args.input)
      counts = (lines
                | 'Wait' >> (beam.ParDo(WaitDoFn())))
      counts | 'Write' >> WriteToText(known_args.output)
      """

      """
      # PATTERN_v2:complex_subprocess
      lines = p | 'Read' >> ReadFromText(known_args.input)
      counts = (lines
                | 'Wait1' >> (beam.ParDo(WaitDoFn()))
                | 'Wait2' >> (beam.ParDo(WaitDoFn()))
                )

      counts | 'Write' >> WriteToText(known_args.output)
      """

      """
      # PATTERN_v3:devide_pcollection
      lines = p | 'Read' >> ReadFromText(known_args.input)

      counts = (lines
                | 'Wait1' >> (beam.ParDo(WaitDoFn()))
                )

      counts2 = (lines
                | 'Wait2' >> (beam.ParDo(WaitDoFn()))
                )

      counts2 | 'Write' >> WriteToText(known_args.output)
      """

      # PATTERN_v4:increase data
      lines = p | 'Read' >> ReadFromText(known_args.input)

      counts = (lines
                | 'Wait1' >> (beam.ParDo(WaitDoFn()))
                | 'Wait2' >> (beam.ParDo(WaitDoFn()))
                )

      counts | 'Write' >> WriteToText(known_args.output)




if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
