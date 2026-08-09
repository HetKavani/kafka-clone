export interface ClusterSummary {
  status: 'healthy' | 'unhealthy';
  brokers_count: number;
  active_brokers_count: number;
  topics_count: number;
}

export interface Broker {
  broker_id: string;
  status: 'active' | 'inactive';
  host?: string;
  port?: number;
}

export interface PartitionMetadata {
  topic: string;
  partition: number;
  owner_broker_id: string;
  leader: string;
  replicas: string[];
  isr: string[];
  next_offset: number;
}

export interface Topic {
  id: number;
  name: string;
  partition_count: number;
  created_at: string;
}

export interface ConsumerGroup {
  group_id: string;
  active_members: string[];
  assignments: Record<string, string[]>; // consumer_id -> partitions
}

export interface ConsumerGroupMember {
  consumer_id: string;
  topics: string[];
}

export interface ConsumerOffset {
  topic: string;
  partition: number;
  offset: number;
  updated_at?: string;
}

export interface LagDetail {
  topic: string;
  partition: number;
  latest_offset: number;
  committed_offset: number;
  lag: number;
}

export interface ConsumerGroupLag {
  group_id: string;
  lag_details: LagDetail[];
  total_lag: number;
}

export interface KafkaXEvent {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: any;
  timestamp: string;
  producer_id: string | null;
}

export interface ReplicationStatus {
  under_replicated_partitions: number;
  out_of_sync_replicas_count: number;
  status: 'healthy' | 'degraded' | 'unhealthy';
}

export interface Metrics {
  events_published: number;
  events_consumed: number;
  avg_publish_latency_ms: number;
  avg_consume_latency_ms: number;
  consumer_lag: number;
  active_brokers: number;
  active_consumers: number;
  partition_count: number;
  replication_status: ReplicationStatus;
}

export interface ActivityLog {
  id: string;
  timestamp: string;
  type: 'heartbeat' | 'publish' | 'commit' | 'isr' | 'election' | 'recovery' | 'system';
  message: string;
}
