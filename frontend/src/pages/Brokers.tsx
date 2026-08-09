import React from 'react';
import { Server } from 'lucide-react';
import type { Broker, PartitionMetadata } from '../types';

interface BrokersProps {
  brokers: Broker[];
  partitions: PartitionMetadata[];
  loading: boolean;
}

export const Brokers: React.FC<BrokersProps> = ({ brokers, partitions, loading }) => {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {brokers.map((broker) => {
          // Calculate partition details for this broker
          const ledPartitions = partitions.filter((p) => p.leader === broker.broker_id);
          const replicatedPartitions = partitions.filter(
            (p) => p.replicas.includes(broker.broker_id) && p.leader !== broker.broker_id
          );
          const totalAssigned = partitions.filter((p) => p.replicas.includes(broker.broker_id));
          const isrCount = totalAssigned.filter((p) => p.isr.includes(broker.broker_id)).length;

          return (
            <div key={broker.broker_id} className="bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col justify-between overflow-hidden">
              <div className="p-6 border-b border-[#1a1c23] bg-[#0c0d13]/30">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center space-x-3">
                    <Server className="h-5 w-5 text-purple-400" />
                    <span className="font-mono text-base font-bold text-white">{broker.broker_id}</span>
                  </div>
                  <span className={`inline-flex items-center space-x-1 px-2 py-0.5 rounded text-[10px] uppercase font-bold ${
                    broker.status === 'active' 
                      ? 'bg-emerald-950/20 text-emerald-400 border border-emerald-800/30' 
                      : 'bg-red-950/20 text-red-400 border border-red-800/30'
                  }`}>
                    <span className={`h-1.5 w-1.5 rounded-full ${broker.status === 'active' ? 'bg-emerald-500' : 'bg-red-500'}`} />
                    <span>{broker.status}</span>
                  </span>
                </div>

                <div className="space-y-1 text-xs font-mono text-gray-400">
                  <div>Host: <span className="text-white">{broker.host || 'N/A'}</span></div>
                  <div>Port: <span className="text-white">{broker.port || 'N/A'}</span></div>
                </div>
              </div>

              <div className="p-6 space-y-4">
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div className="bg-[#12131a] p-3 border border-[#1a1c23] rounded">
                    <div className="text-[10px] text-gray-500 font-mono uppercase">Leader</div>
                    <div className="text-lg font-bold font-mono text-white mt-1">{ledPartitions.length}</div>
                  </div>
                  <div className="bg-[#12131a] p-3 border border-[#1a1c23] rounded">
                    <div className="text-[10px] text-gray-500 font-mono uppercase">Replica</div>
                    <div className="text-lg font-bold font-mono text-white mt-1">{replicatedPartitions.length}</div>
                  </div>
                  <div className="bg-[#12131a] p-3 border border-[#1a1c23] rounded">
                    <div className="text-[10px] text-gray-500 font-mono uppercase">In ISR</div>
                    <div className="text-lg font-bold font-mono text-emerald-400 mt-1">
                      {isrCount}
                      <span className="text-xs text-gray-500 font-normal">/{totalAssigned.length}</span>
                    </div>
                  </div>
                </div>

                {/* Led Partitions Details */}
                {ledPartitions.length > 0 && (
                  <div className="space-y-1.5">
                    <div className="text-[10px] uppercase font-bold text-gray-500 font-mono tracking-wider">
                      Leader For
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {ledPartitions.map((p) => (
                        <span key={`${p.topic}-${p.partition}`} className="bg-purple-950/20 text-purple-400 border border-purple-800/30 text-[10px] font-mono px-2 py-0.5 rounded">
                          {p.topic}/P{p.partition}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* Replicated Partitions Details */}
                {replicatedPartitions.length > 0 && (
                  <div className="space-y-1.5">
                    <div className="text-[10px] uppercase font-bold text-gray-500 font-mono tracking-wider">
                      Replica For
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {replicatedPartitions.map((p) => (
                        <span key={`${p.topic}-${p.partition}`} className="bg-blue-950/20 text-blue-400 border border-blue-800/30 text-[10px] font-mono px-2 py-0.5 rounded">
                          {p.topic}/P{p.partition}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
      
      {brokers.length === 0 && !loading && (
        <div className="text-center py-20 bg-[#0c0d13] border border-[#1a1c23] rounded">
          <Server className="h-8 w-8 text-gray-600 mx-auto mb-3" />
          <h3 className="text-sm font-semibold text-gray-400 font-mono">No registered brokers detected</h3>
          <p className="text-xs text-gray-500 mt-1 max-w-sm mx-auto">
            Ensure your backend brokers are running and sending heartbeats to Redis.
          </p>
        </div>
      )}
    </div>
  );
};
