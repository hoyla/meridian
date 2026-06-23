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

Routes: `/` → latest `index.html` (a tabbed page: Briefing · Tables ·
Methodology · Glossary); `/report.json` → the canonical snapshot; `/data.xlsx`
→ the journalist workbook the Tables tab links to (published beside the
snapshot by `portal_publish`; 404 if a snapshot was made without one);
`/healthz` → liveness (no GCS touch). Responses are gzipped (`GZipMiddleware`)
— the rendered HTML/JSON run to hundreds of KB and Cloud Run doesn't compress
for us.

---

## Deploy (you run these — they need your GCP account, billing, IAM)

> These are account-level actions on **your** GCP project. Set the three vars,
> then run in order. Verify the exact `--iap` / IAP-binding flags against your
> `gcloud` version — direct IAP on Cloud Run is recent and the surface is still
> settling.

> **Current live deployment** (use these exact values, not the convention below):
> project `meridian-500111`, region `europe-west2`, bucket **`meridian-500111-portal`**,
> service `meridian-portal`. Note the live bucket does **not** follow the
> `meridian-portal-snapshots-<project>` naming the snippet below suggests. The
> authoritative source is always the service's own env var:
> `gcloud run services describe meridian-portal --region europe-west2 --format=json`
> → `containers[0].env` → `PORTAL_BUCKET`.

```bash
PROJECT=your-gcp-project
REGION=europe-west2          # London
BUCKET=meridian-portal-snapshots-$PROJECT   # suggested name for a fresh deploy;
                                            # the live deploy uses meridian-500111-portal

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
- **A managed OAuth client won't work here.** IAP's default Google-managed
  OAuth client only serves users *internal to an organization*. A no-org
  personal project with external (`guardian.co.uk`) users **must** supply a
  **custom OAuth client** (Troubleshooting §3) — without it IAP returns `604`
  on every request, however perfect the service-agent and domain grants are.
  This is the single biggest gotcha; everything else is recoverable in minutes.
- The OAuth consent screen will be **External** (a personal project can't be
  "Internal"), and it must be **Published** — in "Testing" it caps at 100
  hand-added users, which won't cover all staff. IAP uses only basic
  email/profile scopes, so publishing shouldn't trigger Google's verification
  review.
- If the `domain:guardian.co.uk` grant has friction cross-org, fall back to a
  **Google Group** or named `user:` emails in step 5.

## Troubleshooting — the gotchas we actually hit

Standing this up cross-org (personal project, Guardian users) surfaced a chain
of non-obvious failures. In the order they bite:

**1. Build fails: `PERMISSION_DENIED … default service account is missing
permissions`.** On newer projects, `--source` deploys build via Cloud Build
running as the *Compute Engine default* service account, which no longer
auto-gets build perms. Grant it once:
```bash
PNUM=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${PNUM}-compute@developer.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.builder"
```

**2. Build says "using Buildpacks" and fails — it's ignoring the Dockerfile.**
`gcloud run deploy --source .` only finds the Dockerfile when run **from
`portal_service/`** (or pass `--source portal_service`). From the repo root it
buildpacks the whole repo and fails.

**3. IAP returns `Error code 604` ("internal error while authorizing").** Two
independent causes — fix both:
- The **IAP service agent** may not exist / may lack invoker:
  ```bash
  gcloud beta services identity create --service=iap.googleapis.com --project="$PROJECT"
  gcloud run services add-iam-policy-binding meridian-portal --region="$REGION" \
    --member="serviceAccount:service-${PNUM}@gcp-sa-iap.iam.gserviceaccount.com" \
    --role="roles/run.invoker"
  ```
- **The big one — the managed OAuth client is invalid for a no-org project with
  external users.** Supply a custom one (Console → Google Auth Platform →
  Clients → Create client → *Web application*; copy id + secret), give it the
  redirect URI in §4, then point IAP at it:
  ```bash
  cat > /tmp/iap-oauth.yaml <<EOF
  accessSettings:
    oauthSettings:
      clientId: <CLIENT_ID>
      clientSecret: <CLIENT_SECRET>
  EOF
  gcloud iap settings set /tmp/iap-oauth.yaml --project="$PROJECT" \
    --resource-type=cloud-run --region="$REGION" --service=meridian-portal
  ```

**4. Sign-in: `Error 400: redirect_uri_mismatch`.** The custom client's
Authorized redirect URI must be exactly, with the **full** client id spliced in
and the **`:handleRedirect`** suffix intact (the console can silently drop it on
paste):
```
https://iap.googleapis.com/v1/oauth/clientIds/<CLIENT_ID>:handleRedirect
```
Allow a few minutes' propagation after saving.

**5. Publish fails: `DefaultCredentialsError` or quota-project permission
denied.** The local `--upload-to-portal` uses Application Default Credentials,
*separate* from your `gcloud` user login. Run `gcloud auth application-default
login` **as the project-owner account** (a `guardian.co.uk` identity has no
rights on a personal project), and **tick every consent checkbox** — the
`cloud-platform` scope is required and the boxes default to unchecked. Confirm
with `gcloud auth application-default set-quota-project "$PROJECT"` (it only
succeeds with the right account).

## Test it before the publish step exists
Manually drop a snapshot in the bucket, then load the service URL (signed in as
a guardian.co.uk account):

```bash
# from a periodic-run bundle that has 04_Portal/
gcloud storage cp <bundle>/04_Portal/index.html   "gs://$BUCKET/latest/index.html"
gcloud storage cp <bundle>/04_Portal/report.json  "gs://$BUCKET/latest/report.json"
```

That manual `gcloud storage cp` is the fallback; the normal path is the
`scrape.py` snapshot/publish commands in the next section.

## Refreshing the live portal — which command when

All snapshot commands run from the **repo root** (not this `portal_service/`
dir), build from the live `gacc` DB, and publish to the live bucket. None record
a `brief_runs` row, so they never advance the subscriber cycle — they only
re-render and republish the *current* release. Prerequisites for a publish:
`GOOGLE_CLOUD_PROJECT=meridian-500111` and Application Default Credentials with
write on the bucket (`gcloud auth application-default login`).

The one decision is **what happens to the LLM takes** (the per-finding leading
questions + the "one other thing worth a look" box). Regenerating them costs API
spend; reusing the prior ones is free but only valid when the content they
interpret hasn't moved.

| Circumstance | Takes flag | LLM $ | What you get |
|---|---|---|---|
| **New data period** — a fresh release | *(use `--periodic-run`)* | — | the normal daily cycle; add `--portal-takes` to generate takes in-cycle |
| **Amend this release — presentation only** (layout, labels, a bug fix; numbers unchanged) | `--portal-reuse-takes` | **free** | deterministic report rebuilt; prior takes carried forward |
| **Amend this release — content changed** (new groups/findings, a data correction) | `--portal-takes` | **pays** | takes regenerated against the new numbers |
| **Republish as-is**, no takes (e.g. takes backend down) | *(neither flag)* | free | deterministic report only; take boxes blank |

`--portal-reuse-takes` and `--portal-takes` are mutually exclusive. Reuse only
carries a take over when the **data_period is unchanged** *and* the take's
**finding still matches** — a finding that shifted (so was superseded to a new
id) drops its take to blank rather than show a stale one. So if you reach for
reuse but the content genuinely moved, you'll see blank takes where the changed
findings are: that's the cue to rerun with `--portal-takes`.

```bash
cd ~/Code/Other_GitHub/meridian          # repo root, NOT portal_service/

# Presentation-only amendment — free, keeps the existing takes:
GOOGLE_CLOUD_PROJECT=meridian-500111 PORTAL_REGION=europe-west2 \
  .venv/bin/python scrape.py --portal-snapshot exports/portal-snapshot \
  --portal-bucket meridian-500111-portal --portal-reuse-takes --portal-warm

# Content changed — pay for fresh takes (re-run the analysers first if you
# added or changed findings):
GOOGLE_CLOUD_PROJECT=meridian-500111 PORTAL_REGION=europe-west2 \
  .venv/bin/python scrape.py --portal-snapshot exports/portal-snapshot \
  --portal-bucket meridian-500111-portal --portal-takes --portal-warm
```

**Preview before going live.** Add `--portal-no-publish` to build locally
*without* publishing — the bucket is still read (so reuse can graft the prior
takes) and the result is baked into the local `index.html`. Open it, check it,
then publish the **same bytes** with `--upload-to-portal` (no rebuild, no extra
LLM spend). Composes with either takes mode:

```bash
# 1. Build + graft prior takes, but hold the publish:
GOOGLE_CLOUD_PROJECT=meridian-500111 \
  .venv/bin/python scrape.py --portal-snapshot exports/portal-snapshot \
  --portal-bucket meridian-500111-portal --portal-reuse-takes --portal-no-publish

# 2. Eyeball it:
open exports/portal-snapshot/04_Portal/index.html

# 3. Happy? Publish the previewed bundle as-is:
GOOGLE_CLOUD_PROJECT=meridian-500111 PORTAL_REGION=europe-west2 \
  .venv/bin/python scrape.py --upload-to-portal exports/portal-snapshot \
  --portal-bucket meridian-500111-portal --portal-warm
```

Why reuse needs the bucket: the page reporters see is the **pre-rendered
`index.html`**, built on the laptop *before* publish — so prior takes are grafted
onto the report at build time (read from the live `latest/report.json`), not
merged into the JSON at upload time.

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
