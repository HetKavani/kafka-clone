import api from './api';
import type { ConsumerGroup, ConsumerGroupMember, ConsumerOffset, ConsumerGroupLag } from '../types';

export const listConsumerGroups = async (): Promise<string[]> => {
  const resp = await api.get<string[]>('/consumer-groups');
  return resp.data;
};

export const getConsumerGroupDetails = async (group: string): Promise<ConsumerGroup> => {
  const resp = await api.get<ConsumerGroup>(`/consumer-groups/${group}`);
  return resp.data;
};

export const getConsumerGroupMembers = async (group: string): Promise<ConsumerGroupMember[]> => {
  const resp = await api.get<ConsumerGroupMember[]>(`/consumer-groups/${group}/members`);
  return resp.data;
};

export const getConsumerGroupOffsets = async (group: string): Promise<ConsumerOffset[]> => {
  const resp = await api.get<ConsumerOffset[]>(`/consumer-groups/${group}/offsets`);
  return resp.data;
};

export const getConsumerGroupLag = async (group: string): Promise<ConsumerGroupLag> => {
  const resp = await api.get<ConsumerGroupLag>(`/consumer-groups/${group}/lag`);
  return resp.data;
};

export const commitOffset = async (
  group: string,
  topic: string,
  partition: number,
  offset: number
): Promise<ConsumerOffset> => {
  const resp = await api.post<ConsumerOffset>(`/consumers/${group}/offsets/commit`, {
    topic,
    partition,
    offset,
  });
  return resp.data;
};

export const sendHeartbeat = async (
  group: string,
  consumerId: string,
  topics: string[]
): Promise<void> => {
  await api.post(`/consumers/${group}/heartbeat`, null, {
    params: {
      consumer_id: consumerId,
      topics,
    },
  });
};

export const getAssignments = async (
  group: string,
  consumerId: string,
  topic: string
): Promise<{ assigned_partitions: number[] }> => {
  const resp = await api.get<{ assigned_partitions: number[] }>(
    `/consumers/${group}/assignments/${consumerId}`,
    { params: { topic } }
  );
  return resp.data;
};
