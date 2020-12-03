import logging
import requests
import json

from google.cloud import bigquery

def sample_function_20201203_post_slack(event, context):

    url = "https://hooks.slack.com/services/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"

    # Slackへの通知の見せ方次第で変わってくるかと思います
    requests.post(url, data=json.dumps({
        'text': 'サンプルBot; to: @Nagao Norihiro',
        'username': 'サンプルBot',
        'icon_emoji': ':robot_face:',
        'link_names': 1,
        "attachments": [
          {
            "fallback": "this is a bot by jao",
            "text": "BigQueryへのロード処理が完了しました",
            "color": "#3399FF"
          }
        ]
    }))
