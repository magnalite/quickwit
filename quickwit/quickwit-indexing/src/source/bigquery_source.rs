use std::{sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_bigquery::{
    client::{ChannelConfig, ClientConfig, ReadTableOption},
    http::table::TableReference,
    storage,
};
use google_cloud_default::bigquery::CreateAuthExt;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::BigQuerySourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use serde_json::{json, Value as JsonValue};
use time::{macros::format_description, OffsetDateTime};
use tokio::{
    sync::Mutex,
    time::{sleep, Instant},
};
use tracing::{info, warn};

use crate::{
    actors::DocProcessor,
    models::{NewPublishLock, PublishLock, RawDocBatch},
};

use super::{Source, SourceActor, SourceContext, SourceExecutionContext, TypedSourceFactory};

/*
    BigQuery streaming ingest strategy

    BigQuery presents some challenges when it comes to ingesting the most recent data:
        1) The streaming buffer will commit rows in a random order and may hold onto rows for
            an indeterminate amount of time, usually <= 2 hours.
        2) We need to use the storage read api to be performant enough however the storage read
            api does not guarantee any ordering.
        3) Rows do not need a unique id or a row number. We need to be able to support tables
            which take any form, including lack of primary key or row numbers.

    To overcome all those challenges we need a way to keep track of which rows we have ingested.
    In order to do this we can move data we intend to ingest to an immutable "staging" table adding
    a hash for the rows data (*a). This table will include all rows which exist in a timestamp range.
    We can then use that timestamp range as our partition checkpoint. eg once we have ingested the
    entire staging table we can move the checkpoint to the final timestamp.

    Once we finish ingesting the staging table we can then query the next timestamp range and merge
    it into the staging table. We want to drop any rows which have the same hash and only keep rows
    which were not in the staging table before. This allows us to have overlapping timestamp ranges
    whilst maintaining exactly once delivery.

    We can now repeat this process to continually move new data into the staging table and then ingesting.

    As the staging table can live for as long as we need it we can gracefully recover in the event
    the source/pipeline/indexer goes offline. We can resume by ingesting the staging table and then
    incrementing the checkpoint as before.

    It is important to note that we need two variables here.
        1) Window width
        2) Query rate

    The window width is the range of the timestamps we use. Typically this should be ~2 hours to ensure
    we include the entire streaming buffer. The query rate is how often we will requery this window to ingest
    new data.

    As an example if we have a window width of 2 hours and a query rate of 10 mins then we check the past 2 hours
    for new data every 10 mins. Additionally we shift the checkpoint by a maximum of the window width. If we are
    ingesting historical data then we can ingest in 2 hour windows with no overlap until we reach the current
    timestamp (or the end of the table) where we then requery at the current timestamp at the query rate with the
    query window overlapping. Importantly a query rate of 10 mins means the ingest latency will be ~10 mins.

    Users can adjust the window width and query rate to suit their needs. A larger window width decreases the
    likelihood we miss rows from the streaming buffer (approaching near 0 at 2 hours) but increases the cost
    in terms of redundant work done by BigQuery. A faster query rate will reduce ingest latency but also increase
    cost due to redundant work. In either case BigQuery does all the redundant work instead of Quickwit.

    *a: Due to relying on hashes we cannot support multiple rows which have the exact same data. In practice
        that should be rare and not a big issue.


    *** For low latency ingest this is a very expensive solution if using editions pricing. On-demand pricing is likely
    *** ok bit still far from ideal. It may be cheaper to do this hashing and comparing in Quickwit although could incur
    *** a significant memory penalty for large streamer buffer delay times.

    As a cheaper alternative we can only ingest up until the beginning of the streaming buffer, then wait for the streaming
    buffer to update and ingest up to the next earliest row in the streaming buffer. This removes all redundant work and
    should be far cheaper. This however limits the minimum ingest latency to the max streaming buffer time. In practice, for
    reasonable volumes of data, the streaming buffer is generally only ~2-3 mins behind real time. It is however not guaranteed
    so ingest latency could vary by up to 2 hours especially for low volume tables.
*/

pub struct BigQuerySourceFactory;

#[async_trait]
impl TypedSourceFactory for BigQuerySourceFactory {
    type Source = BigQuerySource;
    type Params = BigQuerySourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: BigQuerySourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        BigQuerySource::try_new(ctx, params, checkpoint).await
    }
}

pub struct BigQuerySourceState {
    doc_count: u64,
    current_time: OffsetDateTime,
    target_time: OffsetDateTime,
}

pub struct BigQuerySource {
    ctx: Arc<SourceExecutionContext>,
    state: BigQuerySourceState,
    publish_lock: PublishLock,
    client: google_cloud_bigquery::client::Client,
    source_table: TableReference,
    bigquery_consumer: Option<Mutex<storage::Iterator<storage::row::Row>>>,
    partition_id: PartitionId,
    batch_builder: BatchBuilder,
    use_force_commit: bool,
    time_column: String,
}

impl BigQuerySource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: BigQuerySourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let publish_lock = PublishLock::default();

        let (config, _) = ClientConfig::new_with_auth().await.unwrap();
        let client = google_cloud_bigquery::client::Client::new(
            config
                .with_debug(params.enable_debug_output.unwrap_or(false))
                .with_streaming_read_config(ChannelConfig {
                    num_channels: params.read_streams.unwrap_or(4),
                    ..Default::default()
                }),
        )
        .await
        .unwrap();

        let source_table = TableReference {
            project_id: params.project_id.clone(),
            dataset_id: params.dataset_id.clone(),
            table_id: params.table_id.clone(),
        };

        let partition_id = PartitionId::from(format!(
            "{}.{}.{}",
            params.project_id, params.dataset_id, params.table_id
        ));

        let current_time = match checkpoint.is_empty() {
            true => time::OffsetDateTime::parse(
                &params.ingest_start,
                format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
                ),
            )
            .expect("Failed to parse ingest start time"),
            false => time::OffsetDateTime::from_unix_timestamp(
                checkpoint
                    .position_for_partition(&partition_id)
                    .cloned()
                    .unwrap()
                    .as_str()
                    .parse()?,
            )?,
        };

        // TODO: WE CANNOT DO THIS! We need to ensure we do not run into the streaming buffer!
        let target_time = current_time + time::Duration::minutes(1);

        Ok(BigQuerySource {
            ctx,
            state: BigQuerySourceState {
                doc_count: 0,
                current_time,
                target_time,
            },
            publish_lock,
            client,
            source_table,
            bigquery_consumer: None,
            partition_id,
            batch_builder: BatchBuilder::default(),
            use_force_commit: params.force_commit.unwrap_or(false),
            time_column: params.time_column,
        })
    }
}

#[derive(Debug, Default, Clone)]
struct BatchBuilder {
    docs: Vec<Bytes>,
    num_bytes: u64,
    checkpoint_delta: SourceCheckpointDelta,
}

impl BatchBuilder {
    fn build(self, force_commit: bool) -> RawDocBatch {
        RawDocBatch {
            docs: self.docs,
            checkpoint_delta: self.checkpoint_delta,
            force_commit,
        }
    }

    fn clear(&mut self) {
        self.docs.clear();
        self.num_bytes = 0;
        self.checkpoint_delta = SourceCheckpointDelta::default();
    }

    fn push(&mut self, doc: Bytes, num_bytes: u64) {
        self.docs.push(doc);
        self.num_bytes += num_bytes;
    }
}

#[async_trait]
impl Source for BigQuerySource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        Ok(())
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut reached_end_of_batch = false;

        info!("BigQuery pulling batch, docs: {}", self.state.doc_count);

        loop {
            tokio::select! {
                reached_end = self.consume_rows() => {
                    reached_end_of_batch = reached_end?;

                    if reached_end_of_batch {
                        break;
                    }
                }

                _ = sleep(*quickwit_actors::HEARTBEAT / 2) => {
                    warn!("Hit heartbeat deadline!");
                    break;
                }
            }
            ctx.record_progress();
        }

        // While it would be nice to guarantee a batch size we *must* reach the end of the
        // BigQuery read session before commiting. The storage read api gives no guarantees
        // of order so if we used the latest timestamp we may have skipped data which would
        // be lost if we resumed from that checkpoint.
        //
        // To ensure reasonable batch sized we must instead trust the user to configure an
        // appropriate "window size" which is the width of the timestamp range we collect
        // logs from and increment by before reaching the streaming buffer.
        //
        // BigQuery can give us an expected read session size in bytes which we could
        // instead use to dynamically resize the window. It is however only approximate.
        if reached_end_of_batch {
            self.batch_builder
                .checkpoint_delta
                .record_partition_delta(
                    self.partition_id.clone(),
                    Position::from(self.state.current_time.unix_timestamp()),
                    Position::from(self.state.target_time.unix_timestamp()),
                )
                .context("Failed to record partition delta.")?;

            if !self.batch_builder.checkpoint_delta.is_empty() {
                info!(
                    num_docs=%self.batch_builder.docs.len(),
                    num_bytes=%self.batch_builder.num_bytes,
                    num_millis=%now.elapsed().as_millis(),
                    "Sending doc batch to indexer.");
                let message = self.batch_builder.clone().build(self.use_force_commit); // TODO: This clone seems incredibly wasteful?
                ctx.send_message(doc_processor_mailbox, message).await?;
            }

            self.batch_builder.clear();

            self.increment_time_window().await?;
        }

        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &self,
        _checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn name(&self) -> String {
        format!(
            "BigQuerySource{{source_id={}}}",
            self.ctx.source_config.source_id
        )
    }

    fn observable_state(&self) -> JsonValue {
        JsonValue::Object(Default::default())
    }
}

impl BigQuerySource {
    async fn create_bigquery_consumer(&mut self) -> anyhow::Result<()> {
        info!("Creating bigquery consumer");
        let row_restriction = format!(
            "{time_column} >= timestamp_seconds({}) AND {time_column} < timestamp_seconds({})",
            self.state.current_time.unix_timestamp(),
            self.state.target_time.unix_timestamp(),
            time_column = self.time_column,
        );

        let read_options =
            google_cloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions {
                row_restriction,
                ..Default::default()
            };
        let options = Some(ReadTableOption::default().with_session_read_options(read_options));

        self.bigquery_consumer = Some(Mutex::new(
            self.client
                .read_table::<storage::row::Row>(&self.source_table, options)
                .await?,
        ));

        Ok(())
    }

    async fn consume_rows(&mut self) -> anyhow::Result<bool> {
        if self.state.current_time == self.state.target_time {
            info!("Awaiting streaming buffer update...");
            sleep(Duration::from_secs(5)).await;
            self.increment_time_window().await?;
            return Ok(false);
        }

        let mut reached_end_of_batch = false;
        let start_doc_count = self.state.doc_count;

        match &mut self.bigquery_consumer {
            None => self.create_bigquery_consumer().await?,
            Some(consumer) => {
                loop {
                    // TODO: Handle error case here (bigquery api can return errors)
                    if let Some(row) = consumer.get_mut().next().await? {
                        let data = process_row(row)?;
                        let length = data.len() as u64;
                        self.batch_builder.push(data, length);
                        self.state.doc_count += 1;
                    } else {
                        info!("Reached end of rows");
                        reached_end_of_batch = true;
                        self.bigquery_consumer = None;
                        break;
                    }

                    if self.state.doc_count >= start_doc_count + 10000 {
                        break;
                    }
                }
            }
        }

        Ok(reached_end_of_batch)
    }

    async fn increment_time_window(&mut self) -> anyhow::Result<()> {
        info!("Incrementing time window");
        let table_info = self
            .client
            .table()
            .get(
                self.source_table.project_id.as_str(),
                self.source_table.dataset_id.as_str(),
                self.source_table.table_id.as_str(),
            )
            .await?;

        let oldest_entry = match table_info.streaming_buffer {
            Some(buffer) => buffer.oldest_entry_time,
            None => None,
        };

        let mut new_target_time = self.state.target_time.unix_timestamp() + 60; // TODO: Configurable window slide amount
        if let Some(oldest_entry) = oldest_entry {
            let oldest_entry_seconds = oldest_entry as i64 / 1000;

            if oldest_entry_seconds < new_target_time {
                new_target_time = oldest_entry_seconds;
            }
        }

        self.state.current_time = self.state.target_time;
        self.state.target_time = time::OffsetDateTime::from_unix_timestamp(new_target_time)?;

        info!(
            "Updated time slice window: from {} to {}",
            self.state.current_time, self.state.target_time
        );

        Ok(())
    }
}

fn process_row(row: storage::row::Row) -> anyhow::Result<Bytes> {
    let timestamp = row.column::<time::OffsetDateTime>(0)?;
    let data = row.column::<String>(1)?;

    let mut parsed_data: JsonValue = serde_json::from_str(&data)?;

    parsed_data["timestamp"] = json!(timestamp.unix_timestamp_nanos());
    parsed_data.to_string();

    Ok(Bytes::from(parsed_data.to_string()))
}
