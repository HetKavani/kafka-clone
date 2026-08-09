import api from './api';
import type { Topic, KafkaXEvent } from '../types';

export const listTopics = async (): Promise<Topic[]> => {
  const resp = await api.get<Topic[]>('/topics');
  return resp.data;
};

export const createTopic = async (name: string, partitionCount: number): Promise<Topic> => {
  const resp = await api.post<Topic>('/topics/', {
    name,
    partition_count: partitionCount,
  });
  return resp.data;
};

export const getTopic = async (name: string): Promise<Topic> => {
  const resp = await api.get<Topic>(`/topics/${name}`);
  return resp.data;
};

export const deleteTopic = async (name: string): Promise<void> => {
  await api.delete(`/topics/${name}`);
};

export interface PublishRequest {
  value: any;
  key?: string | null;
  partition?: number | null;
  acks?: 'none' | 'leader' | 'all';
}

export const publishEvent = async (
  topic: string,
  req: PublishRequest,
  producerId?: string
): Promise<{ topic: string; partition: number; offset: number; timestamp: string }> => {
  const headers: Record<string, string> = {};
  if (producerId) {
    headers['X-Producer-ID'] = producerId;
  }
  const resp = await api.post(`/topics/${topic}/publish`, req, { headers });
  return resp.data;
};

export interface FetchEventsOptions {
  strategy?: 'earliest' | 'latest' | 'specific' | 'committed' | 'timestamp';
  group_id?: string;
  offset?: number;
  timestamp?: string;
  limit?: number;
}

export const fetchEvents = async (
  topic: string,
  partition: number,
  options: FetchEventsOptions = {}
): Promise<KafkaXEvent[]> => {
  const params: Record<string, any> = {
    partition,
    strategy: options.strategy || 'earliest',
    limit: options.limit || 100,
  };

  if (options.group_id) params.group_id = options.group_id;
  if (options.offset !== undefined) params.offset = options.offset;
  if (options.timestamp) params.timestamp = options.timestamp;

  const resp = await api.get<KafkaXEvent[]>(`/topics/${topic}/events`, { params });
  return resp.data;
};
