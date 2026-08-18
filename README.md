# AWS Streaming Finance Data Pipeline

A near real-time data pipeline that collects daily stock prices for 66 companies, streams them through AWS, stores them in S3, and makes them queryable with SQL.

Built for a graduate Big Data Technologies course (CIS 9760, Baruch College), April–May 2026.

**[Read the full case study →](https://ranxinjiang.github.io/project-aws-pipeline.html)**

---

## Architecture

```
Alpha Vantage API
      ↓
  AWS Lambda          fetch, transform, put_record
      ↓
Kinesis Data Streams  streaming layer (on-demand capacity)
      ↓
Amazon Firehose       buffer and deliver
      ↓
  Amazon S3           partitioned by year / month / day / hour
      ↓
  AWS Glue            crawl and catalog the schema
      ↓
 Amazon Athena        SQL over S3, no server to manage
      ↓
  Analysis            Pandas + Matplotlib
```

## What each service does

| Service | Role |
|---|---|
| Lambda | Retrieves data from the API, transforms each record, writes to the stream |
| Kinesis Data Streams | Receives individual records as they're produced |
| Firehose | Buffers stream records and delivers them to S3 |
| S3 | Persistent storage, partitioned so queries scan less data |
| Glue | Crawls S3 and registers the schema in the Data Catalog |
| Athena | Queries the cataloged files directly with SQL |

## The data

Daily price records for 66 tickers across technology, finance, retail, healthcare, energy, and industrials. The Alpha Vantage daily endpoint returns 100 data points per company, so a complete run produces **6,600 records**.

Each record is flattened to:

```json
{
  "name": "TTWO",
  "ts": "2026-02-11",
  "open_stock": 208.0,
  "close_stock": 203.89,
  "difference": -4.11
}
```

| Field | Meaning |
|---|---|
| `name` | Ticker symbol |
| `ts` | Trading date |
| `open_stock` | Opening price |
| `close_stock` | Closing price |
| `difference` | Close minus open, rounded to 2 decimals |

## Files

| File | Description |
|---|---|
| `lambda_function.py` | The producer — fetches from the API, transforms records, writes to Kinesis |
| `query.sql` | Athena query computing average monthly percentage change per company |
| `stock_analysis.ipynb` | Pandas and Matplotlib analysis of the query results |
| `results.csv` | Exported Athena output (396 rows), input for the notebook |

## Running it

### 1. Set your API key

The Lambda function reads the Alpha Vantage key from an environment variable rather than hardcoding it. In the Lambda console, under **Configuration → Environment variables**, add:

```
ALPHA_VANTAGE_API_KEY = your_key_here
```

Free keys are available at [alphavantage.co](https://www.alphavantage.co/support/#api-key).

### 2. AWS resources

You'll need, in the same region:

- A Kinesis data stream (on-demand capacity is fine)
- A Firehose delivery stream reading from Kinesis, writing to an S3 bucket
- A Glue crawler pointed at that bucket
- An Athena workgroup with a query result location configured

Update `STREAM_NAME` in `lambda_function.py` to match your stream.

### 3. Run

Invoke the Lambda. It loops through all 66 tickers, retrying up to five times per company with a 15-second pause when the API rate-limits, and returns a summary including the record count and any companies that were skipped.

Once Firehose has flushed to S3, run the Glue crawler, then execute `query.sql` in Athena.

### 4. Analyze

Export the Athena results as CSV and upload it to the notebook. `stock_analysis.ipynb` expects a file named `results.csv`.

**Note on reproducibility:** the notebook reads from the exported CSV rather than querying Athena directly, so reproducing the charts requires running the query and exporting the results first. Connecting the notebook straight to Athena or S3 would remove that manual step.

## Notes and limitations

- **Rate limiting** is the main constraint. The free Alpha Vantage tier limits request frequency, which is why the producer retries with backoff and pauses 50ms between `put_record` calls.
- **The `requests` library is installed at runtime** via `pip install --target /tmp`. This works but is slow and fragile — a Lambda layer or a packaged deployment would be the correct approach.
- **"Near real-time"** is the honest description. The source publishes daily bars, so the pipeline streams historical records as they're fetched rather than reacting to live market movement. The architecture would support genuine real-time data unchanged; the constraint is the source, not the design.
- **The analysis metric** — average signed daily change — measures drift rather than volatility, since gains and losses cancel. Measuring volatility would call for a standard deviation or an absolute change.

## Built with

Python · Pandas · Matplotlib · SQL · AWS Lambda · Kinesis Data Streams · Firehose · S3 · Glue · Athena

---

**Ran Xin Jiang** · [Portfolio](https://ranxinjiang.github.io) · [LinkedIn](https://www.linkedin.com/in/ranxin-jiang/)
