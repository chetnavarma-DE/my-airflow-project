from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

SlackWebhookHook(
    slack_webhook_conn_id="slack_webhook_default",
    message=":rocket: Hello from Airflow!",
    username="Airflow Alerts"
).execute()
