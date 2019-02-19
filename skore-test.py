import apache_beam as beam
import argparse
import sys
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import GoogleCloudOptions

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./credentials/skore-key2.json"

# class ProjectDefaultOptions(GoogleCloudOptions):
#   @classmethod
#   def _add_argparse_args(cls,parser):
#     parser.add_argument('--project',
#       default='skore-key')
#     parser.add_argument('--runner',
#       default='DataflowRunner')
#     parser.add_argument('--temp_location',
#       default='gs://skore-teste/tmp')

class GetStories(beam.DoFn):
  def process(self, element):
    if element["type"] == "story":
      element.pop("type")
      yield element   

class GetOnlineStories(beam.DoFn):
  def process(self, element):
    if (element["deleted"]) or (element["dead"]):
      element.pop("deleted")
      element.pop("dead")
      return
    else:
      element.pop("deleted")
      element.pop("dead")
      yield element

class GetGoogleRelatedStories(beam.DoFn):
  def process(self, element):
    if ('google' in element["title"].lower()) or ('google' in element["text"].lower()):
      element.pop("title")
      yield element

# class SanitizeData(beam.DoFn):
#   def process(self, element):

# * id
# 		* title
# 		* text
# 		* url
# 		* ranking
# 		* score
# 		* timestamp
def setSchema():
  table_schema = bigquery.TableSchema()
  id_schema = bigquery.TableFieldSchema()
  id_schema.name = 'id'
  id_schema.type = 'integer'
  id_schema.mode = 'required'
  table_schema.fields.append(id_schema)
  
  text_schema = bigquery.TableFieldSchema()
  text_schema.name = 'text'
  text_schema.type = 'string'
  text_schema.mode = 'nullable'
  table_schema.fields.append(text_schema)
  
  url_schema = bigquery.TableFieldSchema()
  url_schema.name = 'url'
  url_schema.type = 'string'
  url_schema.mode = 'nullable'
  table_schema.fields.append(url_schema)
  
  ranking_schema = bigquery.TableFieldSchema()
  ranking_schema.name = 'ranking'
  ranking_schema.type = 'integer'
  ranking_schema.mode = 'nullable'
  table_schema.fields.append(ranking_schema)
  
  score_schema = bigquery.TableFieldSchema()
  score_schema.name = 'score'
  score_schema.type = 'integer'
  score_schema.mode = 'nullable'
  table_schema.fields.append(score_schema)

  timestamp_schema = bigquery.TableFieldSchema()
  timestamp_schema.name = 'timestamp'
  timestamp_schema.type = 'timestamp'
  timestamp_schema.mode = 'nullable'
  table_schema.fields.append(timestamp_schema)

  return table_schema

def runPipeline():
  
  
  #tableSchema="id:integer,text:string,url:string,ranking:integer,score:integer,timestamp:timestamp"
  # table_spec = bigquery.TableReference(
  #   projectId='skore-key',
  #   datasetId='hacker_news',
  #   tableId='full'
  # )

  sanitizedDataQuery = "SELECT id, text, url, ranking, score, timestamp, type, deleted, dead, title FROM hacker_news.full"
  
  new_table_spec = bigquery.TableReference(
    projectId='skore-new',
    datasetId='hacker_news',
    tableId='newtable'
  )
  result = (p |
   'Read Data' >> beam.io.Read(beam.io.BigQuerySource(query=sanitizedDataQuery, use_standard_sql=True)) |
   'Filter only stories' >> beam.ParDo(GetStories()) |
   'Filter only online stories' >> beam.ParDo(GetOnlineStories()) |
   'Get Google related stories' >> beam.ParDo(GetGoogleRelatedStories()) |
   'Write Results' >> beam.io.WriteToBigQuery(
      new_table_spec,
      schema = setSchema(),
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
  p.run().wait_until_finish()

if __name__ == '__main__':
  p = beam.Pipeline(options=GoogleCloudOptions())
  runPipeline()

