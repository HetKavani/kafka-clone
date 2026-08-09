import React, { useState } from 'react';
import { Layers, Plus, Trash2, Eye } from 'lucide-react';
import type { Topic, PartitionMetadata } from '../types';
import { createTopic, deleteTopic } from '../services/topics';

interface TopicsProps {
  topics: Topic[];
  partitions: PartitionMetadata[];
  onRefresh: () => void;
}

export const Topics: React.FC<TopicsProps> = ({ topics, partitions, onRefresh }) => {
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(1);
  
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const handleCreateTopic = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTopicName.trim()) return;
    setIsSubmitting(true);
    setSubmitError(null);
    try {
      await createTopic(newTopicName.trim(), newTopicPartitions);
      setNewTopicName('');
      setNewTopicPartitions(1);
      setShowCreateModal(false);
      onRefresh();
    } catch (err: any) {
      setSubmitError(err.response?.data?.detail || 'Failed to create topic');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteTopic = async (topicName: string) => {
    if (!confirm(`Are you sure you want to delete topic "${topicName}"?`)) return;
    try {
      await deleteTopic(topicName);
      if (selectedTopic?.name === topicName) {
        setSelectedTopic(null);
      }
      onRefresh();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete topic');
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Topics List Table */}
      <div className={`lg:col-span-2 bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col justify-between overflow-hidden ${selectedTopic ? 'lg:col-span-2' : 'lg:col-span-3'}`}>
        <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30 flex justify-between items-center">
          <div className="flex items-center space-x-2">
            <Layers className="h-4 w-4 text-purple-400" />
            <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
              Topic Inventory
            </h3>
          </div>
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center space-x-1 px-3 py-1.5 bg-purple-600 hover:bg-purple-700 text-white rounded text-xs font-mono font-bold transition duration-150"
          >
            <Plus className="h-3 w-3" />
            <span>Create Topic</span>
          </button>
        </div>

        <div className="p-6">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="text-gray-500 border-b border-[#1a1c23] pb-2">
                <th className="pb-2 font-normal">Topic Name</th>
                <th className="pb-2 font-normal text-center">Partitions</th>
                <th className="pb-2 font-normal">Leaders</th>
                <th className="pb-2 font-normal">ISR Health</th>
                <th className="pb-2 font-normal text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {topics.map((t) => {
                const topicParts = partitions.filter((p) => p.topic === t.name);
                const leaders = Array.from(new Set(topicParts.map((p) => p.leader).filter(Boolean)));
                const isrHealthy = topicParts.every((p) => p.isr.length === p.replicas.length);

                return (
                  <tr key={t.id} className="hover:bg-[#12131a]/50">
                    <td className="py-3 text-white font-medium">
                      <button
                        onClick={() => setSelectedTopic(t)}
                        className="hover:underline text-left"
                      >
                        {t.name}
                      </button>
                    </td>
                    <td className="py-3 text-center text-gray-400">{t.partition_count}</td>
                    <td className="py-3 text-gray-400">
                      {leaders.length > 0 ? leaders.join(', ') : 'None'}
                    </td>
                    <td className="py-3">
                      <span className={`inline-flex items-center space-x-1.5 px-2 py-0.5 rounded text-[10px] uppercase font-bold ${
                        isrHealthy 
                          ? 'bg-emerald-950/20 text-emerald-400 border border-emerald-800/30' 
                          : 'bg-amber-950/20 text-amber-400 border border-amber-800/30'
                      }`}>
                        <span className={`h-1.5 w-1.5 rounded-full ${isrHealthy ? 'bg-emerald-500' : 'bg-amber-500'}`} />
                        <span>{isrHealthy ? 'In Sync' : 'Degraded'}</span>
                      </span>
                    </td>
                    <td className="py-3 text-right space-x-2">
                      <button
                        onClick={() => setSelectedTopic(t)}
                        className="px-2.5 py-1 bg-[#1e1f29] hover:bg-[#282936] text-gray-300 rounded border border-[#2b2d3d] transition duration-150"
                        title="View Topic Details"
                      >
                        <Eye className="h-3.5 w-3.5" />
                      </button>
                      <button
                        onClick={() => handleDeleteTopic(t.name)}
                        className="px-2.5 py-1 bg-red-950/20 hover:bg-red-900/20 text-red-400 rounded border border-red-900/30 transition duration-150"
                        title="Delete Topic"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  </tr>
                );
              })}
              {topics.length === 0 && (
                <tr>
                  <td colSpan={5} className="text-center py-10 text-gray-500">No topics configured in the cluster.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Slide-out Topic Detail View */}
      {selectedTopic && (
        <div className="bg-[#0c0d13] border border-[#1a1c23] rounded overflow-hidden flex flex-col justify-between">
          <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30 flex justify-between items-center">
            <div>
              <span className="text-[10px] text-gray-500 font-mono uppercase tracking-widest block">Topic Detail</span>
              <span className="text-sm font-bold text-white font-mono">{selectedTopic.name}</span>
            </div>
            <button
              onClick={() => setSelectedTopic(null)}
              className="text-gray-400 hover:text-white font-mono text-xs"
            >
              [ Close ]
            </button>
          </div>

          <div className="p-6 flex-1 space-y-4 overflow-y-auto">
            {partitions
              .filter((p) => p.topic === selectedTopic.name)
              .map((p) => {
                const isrHealthy = p.isr.length === p.replicas.length;
                return (
                  <div key={p.partition} className="p-4 bg-[#12131a] border border-[#1a1c23] rounded space-y-3 font-mono text-xs">
                    <div className="flex justify-between items-center border-b border-[#1a1c23] pb-2">
                      <span className="font-bold text-purple-400">Partition {p.partition}</span>
                      <span className={`inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[9px] ${
                        isrHealthy ? 'bg-emerald-950/20 text-emerald-400' : 'bg-amber-950/20 text-amber-400'
                      }`}>
                        <span>ISR:</span>
                        <span className="font-bold">{p.isr.length}/{p.replicas.length}</span>
                      </span>
                    </div>

                    <div className="space-y-1.5 text-gray-400">
                      <div className="flex justify-between">
                        <span>Leader Broker:</span>
                        <span className="text-white font-bold">{p.leader || 'N/A'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Replicas:</span>
                        <span className="text-white">{p.replicas.join(', ')}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>In-Sync Replicas:</span>
                        <span className="text-white">{p.isr.join(', ')}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Next Offset:</span>
                        <span className="text-purple-400 font-bold">{p.next_offset}</span>
                      </div>
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      )}

      {/* Create Topic Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-[#0c0d13] border border-[#1a1c23] w-full max-w-md rounded shadow-2xl overflow-hidden">
            <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30 flex justify-between items-center">
              <h3 className="text-sm font-mono font-bold text-white uppercase tracking-wider">
                Create New Topic
              </h3>
              <button
                onClick={() => setShowCreateModal(false)}
                className="text-gray-400 hover:text-white font-mono text-xs"
              >
                [ Close ]
              </button>
            </div>

            <form onSubmit={handleCreateTopic} className="p-6 space-y-4">
              {submitError && (
                <div className="p-3 bg-red-950/20 border border-red-800/40 text-red-200 text-xs rounded">
                  {submitError}
                </div>
              )}

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase tracking-wider block">
                  Topic Name
                </label>
                <input
                  type="text"
                  required
                  value={newTopicName}
                  onChange={(e) => setNewTopicName(e.target.value.toLowerCase().replace(/[^a-z0-9_.-]/g, ''))}
                  className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                  placeholder="e.g. user_actions"
                />
              </div>

              <div className="space-y-1.5">
                <label className="text-[10px] text-gray-500 font-mono uppercase tracking-wider block">
                  Partition Count
                </label>
                <input
                  type="number"
                  required
                  min={1}
                  max={20}
                  value={newTopicPartitions}
                  onChange={(e) => setNewTopicPartitions(parseInt(e.target.value) || 1)}
                  className="w-full bg-[#12131a] border border-[#1a1c23] rounded p-2 text-xs font-mono text-white focus:outline-none focus:border-purple-500"
                />
              </div>

              <div className="flex justify-end space-x-3 pt-2">
                <button
                  type="button"
                  onClick={() => setShowCreateModal(false)}
                  className="px-4 py-2 border border-[#1a1c23] text-gray-400 hover:text-white rounded text-xs font-mono"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="px-4 py-2 bg-purple-600 hover:bg-purple-700 disabled:opacity-50 text-white font-bold rounded text-xs font-mono"
                >
                  {isSubmitting ? 'Creating...' : 'Create Topic'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};
