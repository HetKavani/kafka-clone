import { useState, useEffect, useRef, useCallback } from 'react';
import { getClusterSummary, getBrokers, getPartitions, getMetrics, getHealth } from '../services/cluster';
import { listTopics } from '../services/topics';
import { listConsumerGroups, getConsumerGroupLag } from '../services/consumers';
import type { ClusterSummary, Broker, PartitionMetadata, Topic, Metrics, ActivityLog, ConsumerGroupLag } from '../types';

export const useClusterData = () => {
  const [summary, setSummary] = useState<ClusterSummary | null>(null);
  const [brokers, setBrokers] = useState<Broker[]>([]);
  const [partitions, setPartitions] = useState<PartitionMetadata[]>([]);
  const [topics, setTopics] = useState<Topic[]>([]);
  const [groups, setGroups] = useState<string[]>([]);
  const [groupLags, setGroupLags] = useState<Record<string, ConsumerGroupLag>>({});
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [health, setHealth] = useState<{ status: string; details: Record<string, string> } | null>(null);
  const [activity, setActivity] = useState<ActivityLog[]>([]);
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  const prevBrokers = useRef<Broker[]>([]);
  const prevPartitions = useRef<PartitionMetadata[]>([]);
  const prevMetrics = useRef<Metrics | null>(null);

  const addLog = useCallback((type: ActivityLog['type'], message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    const id = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    setActivity((prev) => [{ id, timestamp, type, message }, ...prev.slice(0, 99)]);
  }, []);

  const refreshData = useCallback(async (isInitial = false) => {
    if (isInitial) setLoading(true);
    try {
      const [sumData, brokersData, partitionsData, topicsData, groupsData, metricsData, healthData] = await Promise.all([
        getClusterSummary(),
        getBrokers(),
        getPartitions(),
        listTopics(),
        listConsumerGroups(),
        getMetrics(),
        getHealth(),
      ]);

      setSummary(sumData);
      setBrokers(brokersData);
      setPartitions(partitionsData);
      setTopics(topicsData);
      setGroups(groupsData);
      setMetrics(metricsData);
      setHealth(healthData);
      setError(null);

      // Fetch consumer group lags
      const lagPromises = groupsData.map((g) => getConsumerGroupLag(g));
      const lags = await Promise.all(lagPromises);
      const lagMap: Record<string, ConsumerGroupLag> = {};
      lags.forEach((l) => {
        lagMap[l.group_id] = l;
      });
      setGroupLags(lagMap);

      // Generate activity logs based on state changes (skip initial run to avoid log flood)
      if (!isInitial) {
        // Detect broker status changes
        brokersData.forEach((b) => {
          const prev = prevBrokers.current.find((pb) => pb.broker_id === b.broker_id);
          if (prev && prev.status !== b.status) {
            addLog(
              'system',
              `Broker ${b.broker_id} is now ${b.status.toUpperCase()}`
            );
          }
        });

        // Detect partition leader & ISR changes
        partitionsData.forEach((p) => {
          const prev = prevPartitions.current.find(
            (pp) => pp.topic === p.topic && pp.partition === p.partition
          );
          if (prev) {
            if (prev.leader !== p.leader) {
              addLog(
                'election',
                `Partition ${p.topic}/P${p.partition} leader changed: ${prev.leader} -> ${p.leader}`
              );
            }
            const prevIsr = [...prev.isr].sort().join(',');
            const currIsr = [...p.isr].sort().join(',');
            if (prevIsr !== currIsr) {
              addLog(
                'isr',
                `Partition ${p.topic}/P${p.partition} ISR updated: [${p.isr.join(', ')}]`
              );
            }
          }
        });

        // Detect publish/consume count changes
        if (metricsData && prevMetrics.current) {
          const pubDiff = metricsData.events_published - prevMetrics.current.events_published;
          if (pubDiff > 0) {
            addLog('publish', `${pubDiff} event(s) published to cluster`);
          }
          const conDiff = metricsData.events_consumed - prevMetrics.current.events_consumed;
          if (conDiff > 0) {
            addLog('commit', `${conDiff} event(s) processed by consumer groups`);
          }
        }
      }

      // Update refs
      prevBrokers.current = brokersData;
      prevPartitions.current = partitionsData;
      prevMetrics.current = metricsData;

    } catch (err: any) {
      console.error('Failed to poll cluster data:', err);
      setError('Unable to connect to KafkaX API. Confirm backend is online.');
    } finally {
      if (isInitial) setLoading(false);
    }
  }, [addLog]);

  useEffect(() => {
    refreshData(true);
    const interval = setInterval(() => {
      refreshData(false);
    }, 4000); // Poll every 4 seconds

    return () => clearInterval(interval);
  }, [refreshData]);

  return {
    summary,
    brokers,
    partitions,
    topics,
    groups,
    groupLags,
    metrics,
    health,
    activity,
    loading,
    error,
    refresh: () => refreshData(true),
  };
};
