import api from './api';
import type { ClusterSummary, Broker, PartitionMetadata, Metrics } from '../types';

export const getClusterSummary = async (): Promise<ClusterSummary> => {
  const resp = await api.get<ClusterSummary>('/cluster');
  return resp.data;
};

export const getBrokers = async (): Promise<Broker[]> => {
  const resp = await api.get<Broker[]>('/cluster/brokers');
  return resp.data;
};

export const getPartitions = async (): Promise<PartitionMetadata[]> => {
  const resp = await api.get<PartitionMetadata[]>('/cluster/partitions');
  return resp.data;
};

export const getMetrics = async (): Promise<Metrics> => {
  const resp = await api.get<Metrics>('/metrics');
  return resp.data;
};

export const getHealth = async (): Promise<{ status: string; details: Record<string, string> }> => {
  const resp = await api.get<{ status: string; details: Record<string, string> }>('/health');
  return resp.data;
};
