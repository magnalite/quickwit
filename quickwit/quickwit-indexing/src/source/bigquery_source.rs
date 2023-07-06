use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_bigquery::{
    client::{ChannelConfig, ClientConfig, ReadTableOption},
    http::table::TableReference,
    storage,
};
use google_cloud_default::bigquery::CreateAuthExt;
use google_cloud_gax::{grpc::Code, retry::RetrySetting};
use google_cloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::BigQuerySourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use serde_json::{json, Value as JsonValue};
use time::{macros::format_description, OffsetDateTime};
use tokio::{
    task::JoinHandle,
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

    To overcome those challenges we only ingest up until the beginning of the streaming buffer,
    then wait for the streaming buffer to update and ingest up to the next earliest row in the
    streaming buffer. This limits the minimum ingest latency to the max streaming buffer time
    which is not ideal but it is efficient. In practice, for reasonable volumes of data, the
    streaming buffer is generally only ~2-3 mins behind real time. It is however not guaranteed
    so ingest latency could vary by up to 2 hours especially for low volume tables.

    We ingest data in small batches using timestamp windows. This way we can guarantee we
    collect every row without having to ingest a huge amount of data. Each window is
    submitted as a batch and follows the usual checkpoint semantics nicely. A pool of
    consumers are created to concurrently pull multiple batches but they are delivered
    to the DocProcessor in order. This allows us to achieve high throughput by avoiding
    the read session initialisation latency whilst delivering smaller batches to the
    DocProcessor.
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
    batches: VecDeque<JoinHandle<anyhow::Result<BatchBuilder>>>,
    last_known_streaming_buffer_time: Option<i64>,
}

pub struct BigQuerySource {
    ctx: Arc<SourceExecutionContext>,
    state: BigQuerySourceState,
    publish_lock: PublishLock,
    client: google_cloud_bigquery::client::Client,
    source_table: TableReference,
    partition_id: PartitionId,
    use_force_commit: bool,
    time_column: String,
    time_window_size: i64,
    ingest_end: Option<OffsetDateTime>,
    read_streams: usize,
}

impl BigQuerySource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: BigQuerySourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let publish_lock = PublishLock::default();
        let read_streams = params.read_streams.unwrap_or(4);

        let (config, _) = ClientConfig::new_with_auth().await.unwrap();
        let client = google_cloud_bigquery::client::Client::new(
            config
                .with_debug(params.enable_debug_output.unwrap_or(false))
                .with_streaming_read_config(ChannelConfig {
                    num_channels: read_streams,
                    // TODO: Ensure 5 second timeouts are ok? This seems extremely low
                    // however if this is ok it simplifies our ingestion logic.
                    //
                    // The motivation behind this is BigQuery can sometimes be extremely
                    // slow to send rows which can trigger us to bail attempting to read it
                    // as tokio::select! enforces a quickwit_actors::HEARTBEAT deadline.
                    //
                    // That isn't ideal as it is hard to guarantee we didn't drop any rows
                    // unless we restart the entire batch.
                    // (the repurcussions of tokio::select! confuse me, pls help)
                    //
                    // It is unclear why BigQuery sometimes delays sending rows. It could
                    // be due to intentional throttling on the gRPC channels, some bug in
                    // the api or (unlikely) the storage layer needing some time to find rows.
                    connect_timeout: Some(Duration::from_secs(5)),
                    timeout: Some(Duration::from_secs(5)),
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
            "{}:{}.{}.{}",
            ctx.source_config.source_id, params.project_id, params.dataset_id, params.table_id
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

        let ingest_end = params.ingest_end.map(|timestamp| {
            time::OffsetDateTime::parse(
                &timestamp,
                format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
                ),
            )
            .expect("Failed to parse ingest_end")
        });

        Ok(BigQuerySource {
            ctx,
            state: BigQuerySourceState {
                doc_count: 0,
                current_time,
                // Here we set the target_time = current time and allow increment_time_window
                // to correctly increment the target time respecting the streaming buffer
                target_time: current_time,
                batches: VecDeque::new(),
                last_known_streaming_buffer_time: None,
            },
            publish_lock,
            client,
            source_table,
            partition_id,
            use_force_commit: params.force_commit.unwrap_or(false),
            time_column: params.time_column,
            // TODO: We can likely dynamically adjust the time_window_size depending on
            // read session size predictions given by BigQuery or by using previous read
            // sessions as a reference. Finding a suitable time_window_size dynamically
            // is far more preferable than a blind one-size-fits-all number as row density
            // can change significantly through a table and is also generally confusing.
            time_window_size: params.time_window_size.unwrap_or(60),
            ingest_end,
            read_streams,
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
    fn build(&mut self, force_commit: bool) -> RawDocBatch {
        // mem::take lets us take the vec of docs and replace it with an empty vec
        // clearing the batch on build
        self.num_bytes = 0;
        RawDocBatch {
            docs: std::mem::take(&mut self.docs),
            checkpoint_delta: std::mem::take(&mut self.checkpoint_delta),
            force_commit,
        }
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
        let mut batch = None;

        {
            let incoming_batch = self.threaded_consume_rows();
            let emergency_deadline = sleep(Duration::from_secs(600));

            tokio::pin!(incoming_batch);
            tokio::pin!(emergency_deadline);

            loop {
                tokio::select! {
                    pulled_batch = &mut incoming_batch => {
                        batch = pulled_batch?;
                        break;
                    }
                    _ = sleep(*quickwit_actors::HEARTBEAT / 2) => { ctx.record_progress() }
                    _ = &mut emergency_deadline => {
                        warn!("Hit emergency deadline! BigQuery source stalled!");
                        break;
                    }
                }
            }
        }

        // While it would be nice to guarantee a batch size we *must* reach the end of the
        // BigQuery read session before commiting. The storage read api gives no guarantees
        // of order so if we used the latest timestamp we may have skipped data which would
        // be lost if we resumed from that checkpoint.
        //
        // To ensure reasonable batch sizes we must instead trust the user to configure an
        // appropriate "window size" which is the width of the timestamp range we collect
        // logs from and increment by before reaching the streaming buffer.
        //
        // BigQuery can give us an expected read session size in bytes which we could
        // instead use to dynamically resize the window. It is however only approximate.
        if let Some(batch) = &mut batch {
            info!(
                num_docs=%batch.docs.len(),
                num_bytes=%batch.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "Sending doc batch to indexer.");

            let message = batch.build(self.use_force_commit);
            ctx.send_message(doc_processor_mailbox, message).await?;
        } else if let Some(ingest_end) = self.ingest_end {
            if self.state.current_time >= ingest_end {
                info!("Reached end of ingestion window!");
                ctx.send_exit_with_success(doc_processor_mailbox).await?;
                return Err(ActorExitStatus::Success);
            }
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
    // Manages a pool of consumers and returns the next batch when it is ready.
    // Ok(None) is returned if we have reached the streaming buffer. In this case
    // it will sleep before returning to ensure we don't spam api requests unnecessarily.
    async fn threaded_consume_rows(&mut self) -> anyhow::Result<Option<BatchBuilder>> {
        if self.state.current_time == self.state.target_time {
            if !self.state.batches.is_empty() {
                return Ok(Some(self.state.batches.pop_front().unwrap().await??));
            }

            info!("Awaiting streaming buffer update...");
            sleep(Duration::from_secs(5)).await;
            self.increment_time_window().await?;
            return Ok(None);
        }

        while self.state.batches.len() < self.read_streams {
            if self.state.current_time == self.state.target_time {
                break;
            }

            let mut batch = BatchBuilder::default();
            batch
                .checkpoint_delta
                .record_partition_delta(
                    self.partition_id.clone(),
                    Position::from(self.state.current_time.unix_timestamp()),
                    Position::from(self.state.target_time.unix_timestamp()),
                )
                .context("Failed to record partition delta.")?;

            let current_time = self.state.current_time;
            let target_time = self.state.target_time;
            let time_column = self.time_column.clone();
            let client = self.client.clone();
            let source_table = self.source_table.clone();

            self.state.batches.push_back(tokio::spawn(async move {
                let consumer = create_bigquery_consumer(
                    current_time,
                    target_time,
                    time_column,
                    client,
                    &source_table,
                )
                .await
                .unwrap();
                pull_consumer(consumer, batch).await
            }));

            self.increment_time_window().await?;
        }

        Ok(Some(self.state.batches.pop_front().unwrap().await??))
    }

    // Increments the time window ensuring we do not run into the streaming buffer
    // If we have ingested up to the streaming buffer then that is indicated by
    // setting current_time == target_time
    async fn increment_time_window(&mut self) -> anyhow::Result<()> {
        let mut streaming_buffer_time = None;

        if let Some(cached_time) = self.state.last_known_streaming_buffer_time {
            if cached_time > self.state.target_time.unix_timestamp() + self.time_window_size {
                streaming_buffer_time = Some(cached_time);
            }
        }

        if streaming_buffer_time.is_none() {
            let table_info = self
                .client
                .table()
                .get(
                    self.source_table.project_id.as_str(),
                    self.source_table.dataset_id.as_str(),
                    self.source_table.table_id.as_str(),
                )
                .await?;

            streaming_buffer_time = match table_info.streaming_buffer {
                Some(buffer) => buffer.oldest_entry_time.map(|x| x as i64),
                None => None,
            };

            self.state.last_known_streaming_buffer_time = streaming_buffer_time;
        }

        let mut new_target_time = self.state.target_time.unix_timestamp() + self.time_window_size;
        if let Some(oldest_entry) = streaming_buffer_time {
            let oldest_entry_seconds = oldest_entry / 1000;

            if oldest_entry_seconds < new_target_time {
                new_target_time = oldest_entry_seconds;
            }
        }

        if let Some(ingest_end) = self.ingest_end {
            let ingest_end_timestamp = ingest_end.unix_timestamp();
            if ingest_end_timestamp < new_target_time {
                new_target_time = ingest_end_timestamp;
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

    Ok(Bytes::from(parsed_data.to_string()))
}

async fn pull_consumer(
    mut consumer: storage::Iterator<storage::row::Row>,
    mut batch: BatchBuilder,
) -> anyhow::Result<BatchBuilder> {
    // TODO: Handle error case here (bigquery api can return errors)
    while let Some(row) = consumer.next().await? {
        let data = process_row(row)?;
        let length = data.len() as u64;
        batch.push(data, length);
        //self.state.doc_count += 1;
    }

    Ok(batch)
}

async fn create_bigquery_consumer(
    current_time: OffsetDateTime,
    target_time: OffsetDateTime,
    time_column: String,
    client: google_cloud_bigquery::client::Client,
    source_table: &TableReference,
) -> anyhow::Result<storage::Iterator<storage::row::Row>> {
    let row_restriction = format!(
        "{time_column} >= timestamp_seconds({}) AND {time_column} < timestamp_seconds({})",
        current_time.unix_timestamp(),
        target_time.unix_timestamp(),
        time_column = time_column,
    );

    let read_options = TableReadOptions {
        row_restriction,
        ..Default::default()
    };

    let retry_options = RetrySetting {
        from_millis: 10,
        max_delay: Some(Duration::from_secs(1)),
        factor: 1u64,
        take: 5,
        codes: vec![Code::Unavailable, Code::Unknown],
    };

    let options = Some(
        ReadTableOption::default()
            .with_session_read_options(read_options)
            .with_session_retry_setting(retry_options.clone())
            .with_read_rows_retry_setting(retry_options),
    );

    Ok(client
        .read_table::<storage::row::Row>(source_table, options)
        .await?)
}
