create table secret_records (
    id uuid primary key,
    purpose varchar(255) not null,
    ciphertext varchar(4096) not null,
    algorithm varchar(255) not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table cluster_registry (
    id uuid primary key,
    version bigint not null,
    display_name varchar(255) not null,
    description varchar(2000),
    bootstrap_servers varchar(2000) not null,
    controller_bootstrap_endpoints varchar(2000),
    security_protocol varchar(32) not null,
    sasl_mechanism varchar(64),
    username varchar(128),
    credential_secret_id uuid,
    truststore_secret_id uuid,
    keystore_secret_id uuid,
    client_properties_allowlist_json varchar(4000) not null,
    environment varchar(200),
    owner_team varchar(200),
    tags_json varchar(4000),
    enabled boolean not null,
    connection_timeout_ms bigint not null,
    request_timeout_ms bigint not null,
    operation_timeout_ms bigint not null,
    last_successful_validation_at timestamptz,
    last_validation_error_summary varchar(2000),
    observed_kafka_cluster_id varchar(200),
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create unique index ux_cluster_registry_observed_kafka_cluster_id
    on cluster_registry (observed_kafka_cluster_id)
    where observed_kafka_cluster_id is not null;

create table operation_records (
    id uuid primary key,
    cluster_id uuid not null,
    operation_type varchar(128) not null,
    target_resource_name varchar(256),
    current_state varchar(40) not null,
    requested_by varchar(128),
    approved_by varchar(128),
    idempotency_key varchar(128),
    dry_run boolean not null,
    requested_input_json varchar(12000),
    normalized_plan_json varchar(12000),
    dry_run_report_json varchar(12000),
    progress_json varchar(12000),
    failure_category varchar(40),
    failure_details varchar(4000),
    retry_count integer not null,
    cancellation_requested boolean not null,
    cancelled boolean not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    started_at timestamptz,
    completed_at timestamptz,
    lease_owner varchar(128),
    lease_expires_at timestamptz,
    version bigint not null
);

create index ix_operation_records_cluster_id on operation_records (cluster_id);
create index ix_operation_records_state_lease on operation_records (current_state, lease_expires_at);
create unique index ux_operation_records_cluster_idempotency on operation_records (cluster_id, idempotency_key) where idempotency_key is not null;
alter table operation_records
    add constraint fk_operation_records_cluster foreign key (cluster_id) references cluster_registry (id) on delete cascade;

create table operation_events (
    id uuid primary key,
    operation_id uuid not null,
    event_type varchar(80) not null,
    message varchar(2000) not null,
    created_at timestamptz not null,
    sequence_number bigint not null
);

create index ix_operation_events_operation_id on operation_events (operation_id, sequence_number);
alter table operation_events
    add constraint fk_operation_events_operation foreign key (operation_id) references operation_records (id) on delete cascade;
