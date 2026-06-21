# Meridian portal — Cloud Run service

A thin FastAPI app that serves the latest **published snapshot** from a GCS
bucket, behind **IAP** (Guardian-domain sign-in). Read-only v1; scales to zero;
no load balancer. The live analytical DB is never a dependency — the portal
only reads snapshots the laptop pipeline publishes to the bucket.

```
laptop  --periodic-run → 04_Portal/{report.json,index.html}
        → --upload-to-portal → GCS bucket (latest/…)
                                   │
Cloud Run (this app) ──reads latest──┘ ── serves ──▶ IAP (domain:guardian.co.uk) ──▶ reporter
```

Routes: `/` → latest `index.html`; `/report.json` → the canonical snapshot;
`/healthz` → liveness (no GCS touch).

---

## Deploy (you run these — they need your GCP account, billing, IAM)

> These are account-level actions on **your** GCP project. Set the three vars,
> then run in order. Verify the exact `--iap` / IAP-binding flags against your
> `gcloud` version — direct IAP on Cloud Run is recent and the surface is still
> settling.

```bash
PROJECT=your-gcp-project
REGION=europe-west2          # London
BUCKET=meridian-portal-snapshots-$PROJECT

gcloud config set project "$PROJECT"

# 1. Snapshot bucket (once)
gcloud storage buckets create "gs://$BUCKET" \
  --location="$REGION" --uniform-bucket-level-access

# 2. Deploy the service (Cloud Build builds the Dockerfile). Private by default.
#    --min-instances=1 keeps one warm instance through the launch window so
#    reporters hit no cold-start delay (see "Warm at launch" below; flip to 0
#    once they've had their look).
gcloud run deploy meridian-portal \
  --source . --region "$REGION" \
  --set-env-vars PORTAL_BUCKET="$BUCKET" \
  --min-instances=1 \
  --no-allow-unauthenticated

# 3. Let the service's runtime service account READ the bucket
SA=$(gcloud run services describe meridian-portal --region "$REGION" \
       --format='value(spec.template.spec.serviceAccountName)')
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" --role="roles/storage.objectViewer"

# 4. Turn on IAP directly on the service (GA — no load balancer).
#    Easiest in Console: the service → Security → Require authentication → IAP.
#    CLI equivalent (confirm the flag name in your gcloud version):
# gcloud beta run services update meridian-portal --region "$REGION" --iap

# 5. Grant Guardian staff (the whole Workspace domain) access through IAP
gcloud iap web add-iam-policy-binding \
  --resource-type=cloud-run --service=meridian-portal --region="$REGION" \
  --member="domain:guardian.co.uk" \
  --role="roles/iap.httpsResourceAccessor"
```

### The cross-org auth wrinkle (worth knowing before step 4/5)
Your GCP project is under your **personal** Google account; `guardian.co.uk` is
a **separate** Workspace org. So:
- The OAuth consent screen will be **External** (a personal project can't be
  "Internal"), and it must be **Published** — in "Testing" it caps at 100
  hand-added users, which won't cover all staff. IAP uses only basic
  email/profile scopes, so publishing shouldn't trigger Google's verification
  review.
- If the `domain:guardian.co.uk` grant has friction cross-org, fall back to a
  **Google Group** or named `user:` emails in step 5.

## Test it before the publish step exists
Manually drop a snapshot in the bucket, then load the service URL (signed in as
a guardian.co.uk account):

```bash
# from a periodic-run bundle that has 04_Portal/
gcloud storage cp <bundle>/04_Portal/index.html   "gs://$BUCKET/latest/index.html"
gcloud storage cp <bundle>/04_Portal/report.json  "gs://$BUCKET/latest/report.json"
```

Automating that upload is the next step (`scrape.py --upload-to-portal`).

## Warm at launch, cool to save money
A report's first hours are ~90% of its lifetime traffic — reporters reading
fresh material — and that's exactly when a scale-to-zero cold start (a few
seconds) hurts. So run the launch window with **one warm instance**, then scale
to zero once the audience has had its look.

- **On each new report publish** — keep `--min-instances=1` (re-set it if a
  previous report's window left it at 0):
  ```bash
  gcloud run services update meridian-portal --region "$REGION" --min-instances=1
  ```
  (The publish step, `--upload-to-portal`, can do this automatically on each
  publish — opt-in — so you only ever flip it back down by hand.)
- **Once reporters have seen it** — scale to zero to drop the warm-idle cost:
  ```bash
  gcloud run services update meridian-portal --region "$REGION" --min-instances=0
  ```

Declarative equivalent, if you keep a service YAML: the
`autoscaling.knative.dev/minScale` annotation (`"1"` warm / `"0"` cold).

## Cost
Direct IAP (no load balancer) + Cloud Run scale-to-zero (free tier covers an
internal portal) + a few MB in GCS ≈ **~$0/month** at rest. The one cost lever
is the warm instance: `--min-instances=1` is roughly a **few dollars a month**
(one idle instance, mostly billed for memory under the default request-time CPU
model); dropping to `0` after the launch window removes it. IAP itself is free.
