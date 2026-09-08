# Setup: AWS and Snowflake

About 40 minutes end to end, most of it waiting on Snowflake's trial email.
Cost at this volume is a few cents of S3 storage and, with `AUTO_SUSPEND=60`,
a negligible number of Snowflake credits from the free trial.

## 1. Python

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Spark needs **Java 17**. Check with `java -version`. On macOS:
`brew install openjdk@17`. On Ubuntu: `sudo apt install openjdk-17-jdk`.

## 2. AWS

Create a bucket (names are globally unique, so add your name):

```bash
aws s3 mb s3://nyc-trip-platform-<yourname> --region us-east-1
```

Create an IAM user for the pipeline with programmatic access and a policy
scoped to that one bucket — not `AmazonS3FullAccess`. Least privilege is a
habit worth building, and a reviewer will notice:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::nyc-trip-platform-<yourname>",
      "arn:aws:s3:::nyc-trip-platform-<yourname>/*"
    ]
  }]
}
```

Put the access key, secret and bucket name into `.env`.

## 3. Snowflake

Sign up for a trial at signup.snowflake.com — 30 days, $400 of credits, no card.
Pick AWS and the same region as your bucket so you avoid cross-region transfer.

Your account identifier is in the URL, formatted like `abc12345.us-east-1`.
Put it and your credentials in `.env`.

Then in a Snowflake worksheet, run `snowflake/01_setup.sql` (edit the last line
to your username first).

## 4. Connect Snowflake to S3

This is the fiddly bit and it is worth doing properly rather than pasting AWS
keys into Snowflake.

1. In AWS, create an IAM role `snowflake-nyc-role` with the bucket policy above.
   For the trust relationship, put your own account as principal for now — you
   will replace it in step 3.
2. In Snowflake, run the `CREATE STORAGE INTEGRATION` and `DESC INTEGRATION`
   statements from `snowflake/02_storage_integration.sql`. Copy the values of
   `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.
3. Back in AWS, edit the role's trust policy to allow exactly that ARN, with the
   external ID as a condition:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"}}
  }]
}
```

4. Run the rest of `02_storage_integration.sql` to create the stage, then
   `LIST @CURATED_STAGE;`. If it lists files, the trust relationship works.

The external ID is there to prevent the confused deputy problem — it stops
someone else's Snowflake account from assuming your role even if they somehow
learned the ARN. Worth knowing why it exists.

## 5. dbt

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt && dbt debug
```

`dbt debug` should report all checks passed before you go further.

## 6. Run it

```bash
make download    # TLC files -> S3 raw
make curate      # PySpark -> S3 curated + quarantine
make load        # COPY INTO Snowflake
make dbt         # build and test the star schema
```

## Troubleshooting

**Spark: `java.lang.UnsupportedClassVersionError`** — wrong Java. Needs 17.

**Spark: `ClassNotFoundException: S3AFileSystem`** — the hadoop-aws jars are
downloading on first run. Give it a minute; they're cached afterwards.

**Spark hangs or the machine swaps** — lower `SPARK_DRIVER_MEMORY` to `2g` and
cut `TLC_MONTHS` to a single month while you debug.

**Snowflake `COPY INTO` loads 0 rows** — it's idempotent and skips files it has
already loaded. Add `FORCE = TRUE` to reload deliberately.
