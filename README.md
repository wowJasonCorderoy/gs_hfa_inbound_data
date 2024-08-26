# gs_hfa_inbound_data
Function that pulls in relevant hilton inbound data into bigquery.


Make sure you're in the correct gcp project:
gcloud config get-value project
if need to change do something like this: gcloud config set project gcp-wow-pvc-grnstck-prod


gcloud functions deploy hfa_inbound --entry-point=run --runtime=python39  --allow-unauthenticated --memory=4096MB --min-instances=1 --max-instances=100 --timeout=300 --trigger-event=google.storage.object.finalize --trigger-resource=hfa_inbound


