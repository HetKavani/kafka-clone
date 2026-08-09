import React, { useState, useEffect } from 'react';
import { Search, Play, Send, Database } from 'lucide-react';
import type { Topic, PartitionMetadata, KafkaXEvent } from '../types';
import { fetchEvents, publishEvent } from '../services/topics';
import type { FetchEventsOptions } from '../services/topics';

interface EventsProps {
  topics: Topic[];
  partitions: PartitionMetadata[];
  onRefresh: () => void;
}

export const Events: React.FC<EventsProps> = ({ topics, partitions, onRefresh }) => {
  const [activeTab, setActiveTab] = useState<'explorer' | 'replay'>('explorer');

  // Input states
  const [selectedTopic, setSelectedTopic] = useState('');
  const [selectedPartition, setSelectedPartition] = useState(0);
  const [events, setEvents] = useState<KafkaXEvent[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Publish Form States
  const [pubKey, setPubKey] = useState('');
  const [pubValue, setPubValue] = useState('');
  const [pubAcks, setPubAcks] = useState<'none' | 'leader' | 'all'>('leader');
  const [pubProducerId, setPubProducerId] = useState('');
  const [pubStatus, setPubStatus] = useState<string | null>(null);

  // Replay Form States
  const [replayStrategy, setReplayStrategy] = useState<'earliest' | 'latest' | 'specific' | 'timestamp'>('earliest');
  const [replayOffset, setReplayOffset] = useState<number>(0);
  const [replayTimestamp, setReplayTimestamp] = useState('');
  const [replayLimit, setReplayLimit] = useState(50);

  // Load partitions for selected topic
  const topicPartitions = partitions.filter((p) => p.topic === selectedTopic);

  // Set default selection
  useEffect(() => {
    if (topics.length > 0 && !selectedTopic) {
      setSelectedTopic(topics[0].name);
    }
  }, [topics, selectedTopic]);

  const loadEvents = async (strategy: FetchEventsOptions['strategy'] = 'earliest', offset?: number, ts?: string) => {
    if (!selectedTopic) return;
    setLoading(true);
    setError(null);
    try {
      const opts: FetchEventsOptions = {
        strategy,
        limit: replayLimit,
      };
      if (strategy === 'specific' && offset !== undefined) {
        opts.offset = offset;
      }
      if (strategy === 'timestamp' && ts) {
        opts.timestamp = ts;
      }

      const data = await fetchEvents(selectedTopic, selectedPartition, opts);
      setEvents(data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to query events from partition');
    } finally {
      setLoading(false);
    }
  };

  const handlePublish = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedTopic) return;
    setPubStatus('Publishing...');
    try {
      let parsedValue;
      try {
        parsedValue = JSON.parse(pubValue);
      } catch (err) {
        parsedValue = pubValue; // Fallback to raw string
      }

      const res = await publishEvent(
        selectedTopic,
        {
          value: parsedValue,
          key: pubKey.trim() || null,
          partition: selectedPartition,
          acks: pubAcks,
        },
        pubProducerId.trim() || undefined
      );

      setPubStatus(`Success! Offset: ${res.offset} (Partition ${res.partition})`);
      setPubValue('');
      setPubKey('');
      onRefresh();
      // Reload events to show newly published
      loadEvents('earliest');
    } catch (err: any) {
      setPubStatus(`Error: ${err.response?.data?.detail || 'Publish failed'}`);
    }
  };

  return (
    <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
      {/* Configuration & Controls */}
      <div className="bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col justify-between overflow-hidden h-fit">
        <div className="border-b border-[#1a1c23] bg-[#0c0d13]/30">
          <div className="flex border-b border-[#1a1c23]">
            <button
              onClick={() => setActiveTab('explorer')}
              className={`flex-1 py-3 text-xs font-mono font-bold uppercase tracking-wider text-center border-r border-[#1a1c23] ${
                activeTab === 'explorer' ? 'bg-[#191a24] text-purple-400 border-b-2 border-b-purple-500' : 'text-gray-400 hover:text-white'
              }`}
            >
              Event Explorer
            </button>
            <button
              onClick={() => setActiveTab('replay')}
              className={`flex-1 py-3 text-xs font-mono font-bold uppercase tracking-wider text-center ${
                activeTab === 'replay' ? 'bg-[#191a24] text-purple-400 border-b-2 border-b-purple-500' : 'text-gray-400 hover:text-white'
              }`}
            >
              Event Replay
            </button>
          </div>
        </div>

        <div className="p-6 space-y-6">
          {/* Target selection */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <label className="text-[10px] text-gray-500 font-mono uppercase block">Topic</label>
              <select
                value={selectedTopic}
                onChange={(e) => {
                  setSelectedTopic(e.target.value);
                  setSelectedPartition(0);
                  setEvents([]);
                }}
                className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none"
              >
                {topics.map((t) => (
                  <option key={t.id} value={t.name}>{t.name}</option>
                ))}
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[10px] text-gray-500 font-mono uppercase block">Partition</label>
              <select
                value={selectedPartition}
                onChange={(e) => {
                  setSelectedPartition(parseInt(e.target.value) || 0);
                  setEvents([]);
                }}
                className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none"
              >
                {topicPartitions.map((p) => (
                  <option key={p.partition} value={p.partition}>P{p.partition}</option>
                ))}
                {topicPartitions.length === 0 && <option value={0}>P0</option>}
              </select>
            </div>
          </div>

          {activeTab === 'explorer' ? (
            /* Explorer: Publish Form */
            <form onSubmit={handlePublish} className="space-y-4 pt-4 border-t border-[#1a1c23]">
              <h4 className="text-[11px] font-mono font-bold uppercase tracking-wider text-purple-400">
                Publish Event
              </h4>

              {pubStatus && (
                <div className="p-2.5 bg-[#12131a] border border-purple-950/40 text-purple-300 text-[10px] font-mono rounded">
                  {pubStatus}
                </div>
              )}

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1.5">
                  <label className="text-[10px] text-gray-500 font-mono uppercase block">Key</label>
                  <input
                    type="text"
                    value={pubKey}
                    onChange={(e) => setPubKey(e.target.value)}
                    className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                    placeholder="Key string"
                  />
                </div>
                <div className="space-y-1.5">
                  <label className="text-[10px] text-gray-500 font-mono uppercase block">Producer ID</label>
                  <input
                    type="text"
                    value={pubProducerId}
                    onChange={(e) => setPubProducerId(e.target.value)}
                    className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                    placeholder="e.g. client-1"
                  />
                </div>
              </div>

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase block">Acks Level</label>
                <div className="grid grid-cols-3 gap-2">
                  {(['none', 'leader', 'all'] as const).map((level) => (
                    <button
                      key={level}
                      type="button"
                      onClick={() => setPubAcks(level)}
                      className={`py-1.5 text-xs font-mono border rounded ${
                        pubAcks === level
                          ? 'bg-purple-950/20 text-purple-400 border-purple-500/50'
                          : 'bg-[#12131a] text-gray-500 border-[#1a1c23] hover:text-white'
                      }`}
                    >
                      {level}
                    </button>
                  ))}
                </div>
              </div>

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase block">Payload Value (JSON/Text)</label>
                <textarea
                  required
                  rows={4}
                  value={pubValue}
                  onChange={(e) => setPubValue(e.target.value)}
                  className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                  placeholder='{"amount": 100, "currency": "USD"}'
                />
              </div>

              <button
                type="submit"
                className="w-full py-2 bg-purple-600 hover:bg-purple-700 text-white rounded font-mono font-bold text-xs flex items-center justify-center space-x-1.5 transition duration-150"
              >
                <Send className="h-3.5 w-3.5" />
                <span>Publish Event</span>
              </button>
            </form>
          ) : (
            /* Replay Form */
            <div className="space-y-4 pt-4 border-t border-[#1a1c23]">
              <h4 className="text-[11px] font-mono font-bold uppercase tracking-wider text-purple-400">
                Replay Config
              </h4>

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase block">Replay Mode Strategy</label>
                <select
                  value={replayStrategy}
                  onChange={(e: any) => setReplayStrategy(e.target.value)}
                  className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none"
                >
                  <option value="earliest">Earliest (Start from offset 0)</option>
                  <option value="latest">Latest (Read newest logs only)</option>
                  <option value="specific">Offset (Start from specific index)</option>
                  <option value="timestamp">Timestamp (Fetch events after date)</option>
                </select>
              </div>

              {replayStrategy === 'specific' && (
                <div className="space-y-1.5">
                  <label className="text-[10px] text-gray-500 font-mono uppercase block">Start Offset</label>
                  <input
                    type="number"
                    min={0}
                    value={replayOffset}
                    onChange={(e) => setReplayOffset(parseInt(e.target.value) || 0)}
                    className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                  />
                </div>
              )}

              {replayStrategy === 'timestamp' && (
                <div className="space-y-1.5">
                  <label className="text-[10px] text-gray-500 font-mono uppercase block">Start Timestamp (ISO format)</label>
                  <input
                    type="text"
                    value={replayTimestamp}
                    onChange={(e) => setReplayTimestamp(e.target.value)}
                    className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                    placeholder="2026-08-09T12:00:00"
                  />
                </div>
              )}

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase block">Limit Events</label>
                <input
                  type="number"
                  min={1}
                  max={500}
                  value={replayLimit}
                  onChange={(e) => setReplayLimit(parseInt(e.target.value) || 50)}
                  className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                />
              </div>

              <button
                type="button"
                onClick={() => loadEvents(replayStrategy, replayOffset, replayTimestamp)}
                className="w-full py-2 bg-purple-600 hover:bg-purple-700 text-white rounded font-mono font-bold text-xs flex items-center justify-center space-x-1.5 transition duration-150"
              >
                <Play className="h-3.5 w-3.5" />
                <span>Start Replay</span>
              </button>
            </div>
          )}

          {activeTab === 'explorer' && (
            <button
              onClick={() => loadEvents('earliest')}
              className="w-full py-2 bg-[#1e1f29] hover:bg-[#282936] text-white border border-[#2b2d3d] rounded font-mono font-bold text-xs flex items-center justify-center space-x-1.5 transition duration-150"
            >
              <Search className="h-3.5 w-3.5" />
              <span>Query Log Files</span>
            </button>
          )}
        </div>
      </div>

      {/* Events Log Output */}
      <div className="xl:col-span-2 bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col justify-between overflow-hidden min-h-[500px]">
        <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30 flex justify-between items-center">
          <div className="flex items-center space-x-2">
            <Database className="h-4 w-4 text-purple-400" />
            <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
              Commit Log Event Stream
            </h3>
          </div>
          <span className="text-[10px] text-gray-500 font-mono uppercase">
            Count: {events.length}
          </span>
        </div>

        <div className="flex-1 p-6 overflow-y-auto space-y-4 max-h-[620px]">
          {loading && (
            <div className="flex flex-col items-center justify-center py-24 space-y-3">
              <div className="h-6 w-6 border-2 border-purple-500 border-t-transparent rounded-full animate-spin" />
              <span className="text-xs font-mono text-gray-500">Querying partition log stream...</span>
            </div>
          )}

          {!loading && events.map((event) => (
            <div
              key={event.offset}
              className="border border-[#1a1c23] bg-[#12131a] rounded overflow-hidden font-mono text-xs"
            >
              {/* Event Header bar */}
              <div className="px-4 py-2 border-b border-[#1a1c23] bg-[#0c0d13] flex flex-wrap justify-between items-center text-gray-500 text-[10px]">
                <div className="space-x-3">
                  <span>Offset: <span className="text-white font-bold">{event.offset}</span></span>
                  {event.key && (
                    <span>Key: <span className="text-purple-400 font-bold">"{event.key}"</span></span>
                  )}
                  {event.producer_id && (
                    <span>Producer: <span className="text-blue-400 font-bold">{event.producer_id}</span></span>
                  )}
                </div>
                <span>{new Date(event.timestamp).toLocaleTimeString()}</span>
              </div>

              {/* Event Payload */}
              <div className="p-4">
                <pre className="text-emerald-400 whitespace-pre-wrap break-all overflow-x-auto text-[10px] leading-tight">
                  {typeof event.value === 'object'
                    ? JSON.stringify(event.value, null, 2)
                    : event.value}
                </pre>
              </div>
            </div>
          ))}

          {!loading && !error && events.length === 0 && (
            <div className="text-center py-24 text-gray-600 italic font-mono text-xs">
              No events loaded. Select topic/partition and click Query or Replay.
            </div>
          )}

          {error && (
            <div className="p-4 bg-red-950/20 border border-red-800/40 text-red-400 text-xs rounded font-mono">
              Error fetching events: {error}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
