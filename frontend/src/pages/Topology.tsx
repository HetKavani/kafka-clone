import React, { useState, useEffect } from 'react';
import { 
  ReactFlow, 
  Controls, 
  Background 
} from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Database, Activity, Cpu } from 'lucide-react';
import type { Broker, PartitionMetadata, ConsumerGroupLag } from '../types';

interface TopologyProps {
  brokers: Broker[];
  partitions: PartitionMetadata[];
  groupLags: Record<string, ConsumerGroupLag>;
}

export const Topology: React.FC<TopologyProps> = ({ brokers, partitions, groupLags }) => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [selectedPartition, setSelectedPartition] = useState<PartitionMetadata | null>(null);

  useEffect(() => {
    const newNodes: Node[] = [];
    const newEdges: Edge[] = [];

    let currentX = 0;
    const treeSpacing = 350; // Horizontal spacing between partition trees

    partitions.forEach((p) => {
      const treeX = currentX;
      currentX += treeSpacing;

      // 1. Leader Node
      const leaderActive = brokers.find((b) => b.broker_id === p.leader)?.status === 'active';
      const leaderNodeId = `leader-${p.topic}-${p.partition}-${p.leader}`;
      
      newNodes.push({
        id: leaderNodeId,
        type: 'default',
        position: { x: treeX, y: 50 },
        data: {
          label: (
            <div className="font-mono text-xs text-left p-1">
              <div className="flex items-center space-x-1 border-b border-[#2b2d3d] pb-1 mb-1 font-bold text-white">
                <Cpu className="h-3 w-3 text-purple-400" />
                <span>{p.leader || 'NO_LEADER'}</span>
              </div>
              <span className={`text-[9px] uppercase font-extrabold ${leaderActive ? 'text-emerald-400' : 'text-red-400 animate-pulse'}`}>
                {leaderActive ? 'ONLINE (LEADER)' : 'OFFLINE'}
              </span>
            </div>
          ),
        },
        style: {
          backgroundColor: '#0c0d13',
          border: `1px solid ${leaderActive ? '#a855f7' : '#ef4444'}`,
          borderRadius: '4px',
          color: '#d1d5db',
          width: 180,
        },
      });

      // 2. Partition Node
      const partitionNodeId = `part-${p.topic}-${p.partition}`;
      const isrHealthy = p.isr.length === p.replicas.length;

      newNodes.push({
        id: partitionNodeId,
        type: 'default',
        position: { x: treeX, y: 170 },
        data: {
          label: (
            <div className="font-mono text-xs text-left p-1 cursor-pointer">
              <div className="flex items-center space-x-1 border-b border-[#2b2d3d] pb-1 mb-1 font-bold text-purple-400">
                <Database className="h-3 w-3" />
                <span>{p.topic}/P{p.partition}</span>
              </div>
              <div className="text-[9px] text-gray-500">Offset: <span className="text-white font-bold">{p.next_offset}</span></div>
              <div className={`text-[9px] font-extrabold mt-1 ${isrHealthy ? 'text-emerald-400' : 'text-amber-400'}`}>
                {isrHealthy ? 'ISR HEALTHY' : 'UNDER-REPLICATED'}
              </div>
            </div>
          ),
        },
        style: {
          backgroundColor: '#12131a',
          border: `1px solid ${isrHealthy ? '#10b981' : '#f59e0b'}`,
          borderRadius: '4px',
          color: '#d1d5db',
          width: 180,
        },
      });

      // Link Leader to Partition
      newEdges.push({
        id: `edge-l-p-${p.topic}-${p.partition}`,
        source: leaderNodeId,
        target: partitionNodeId,
        animated: leaderActive,
        style: { stroke: leaderActive ? '#a855f7' : '#ef4444', strokeWidth: 2 },
      });

      // 3. Followers / Replicas Nodes
      const followers = p.replicas.filter((r) => r !== p.leader);
      followers.forEach((f, fIdx) => {
        const followerActive = brokers.find((b) => b.broker_id === f)?.status === 'active';
        const isSynced = p.isr.includes(f);
        const followerNodeId = `fol-${p.topic}-${p.partition}-${f}`;

        // Space followers horizontally under partition node
        const followerX = treeX + (fIdx - (followers.length - 1) / 2) * 160;

        let statusText = 'ONLINE (ISR)';
        let statusColor = 'text-emerald-400';
        let borderColor = '#3b82f6'; // Replicating blue

        if (!followerActive) {
          statusText = 'OFFLINE';
          statusColor = 'text-red-400 animate-pulse';
          borderColor = '#ef4444';
        } else if (!isSynced) {
          statusText = 'OUT OF SYNC';
          statusColor = 'text-amber-400 animate-pulse';
          borderColor = '#f59e0b';
        }

        newNodes.push({
          id: followerNodeId,
          type: 'default',
          position: { x: followerX, y: 290 },
          data: {
            label: (
              <div className="font-mono text-xs text-left p-1">
                <div className="flex items-center space-x-1 border-b border-[#2b2d3d] pb-1 mb-1 font-bold text-white">
                  <Cpu className="h-3 w-3 text-blue-400" />
                  <span>{f}</span>
                </div>
                <span className={`text-[9px] uppercase font-extrabold ${statusColor}`}>
                  {statusText}
                </span>
              </div>
            ),
          },
          style: {
            backgroundColor: '#0c0d13',
            border: `1px solid ${borderColor}`,
            borderRadius: '4px',
            color: '#d1d5db',
            width: 140,
          },
        });

        // Link Partition to Follower
        newEdges.push({
          id: `edge-p-f-${p.topic}-${p.partition}-${f}`,
          source: partitionNodeId,
          target: followerNodeId,
          animated: followerActive && isSynced,
          style: { stroke: borderColor, strokeWidth: 1.5 },
        });
      });
    });

    setNodes(newNodes);
    setEdges(newEdges);
  }, [brokers, partitions]);

  const handleNodeClick = (_event: React.MouseEvent, node: Node) => {
    if (node.id.startsWith('part-')) {
      const parts = node.id.split('-');
      const topicName = parts[1];
      const partNum = parseInt(parts[2]);
      const found = partitions.find((p) => p.topic === topicName && p.partition === partNum);
      if (found) {
        setSelectedPartition(found);
      }
    }
  };

  // Compute lag for selected partition
  const getPartitionLagInfo = () => {
    if (!selectedPartition) return null;
    let lag = 0;
    Object.values(groupLags).forEach((gl) => {
      const matched = gl.lag_details.find(
        (ld) => ld.topic === selectedPartition.topic && ld.partition === selectedPartition.partition
      );
      if (matched) lag += matched.lag;
    });
    return lag;
  };

  const selectedLag = getPartitionLagInfo();

  return (
    <div className="flex flex-col lg:flex-row gap-6 h-[600px]">
      {/* Topology Canvas */}
      <div className="flex-1 bg-[#0c0d13] border border-[#1a1c23] rounded overflow-hidden relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodeClick={handleNodeClick}
          fitView
          minZoom={0.5}
          maxZoom={1.5}
        >
          <Background color="#1a1c23" gap={16} />
          <Controls className="bg-[#12131a] border border-[#1a1c23] text-white" />
        </ReactFlow>
        
        <div className="absolute top-4 left-4 bg-[#12131a]/90 backdrop-blur-md p-3 border border-[#1a1c23] rounded font-mono text-[10px] space-y-1.5 z-10">
          <div className="font-bold text-gray-400 border-b border-[#1a1c23] pb-1 mb-1">NODE LEGEND</div>
          <div className="flex items-center space-x-2">
            <span className="h-2 w-2 rounded-full bg-purple-500" />
            <span>Leader Broker Node</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span>ISR Healthy Partition</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="h-2 w-2 rounded-full bg-amber-500" />
            <span>Under-Replicated / Out-of-sync</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="h-2 w-2 rounded-full bg-red-500" />
            <span>Offline Broker</span>
          </div>
        </div>
      </div>

      {/* Side Details Inspector */}
      {selectedPartition && (
        <div className="w-full lg:w-80 bg-[#0c0d13] border border-[#1a1c23] rounded p-6 flex flex-col justify-between font-mono text-xs">
          <div className="space-y-5">
            <div className="border-b border-[#1a1c23] pb-3 flex justify-between items-center">
              <div className="flex items-center space-x-2 text-purple-400 font-bold">
                <Database className="h-4 w-4" />
                <span>Partition Inspect</span>
              </div>
              <button 
                onClick={() => setSelectedPartition(null)}
                className="text-gray-500 hover:text-white"
              >
                [ Clear ]
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Topic</span>
                <span className="text-white font-bold">{selectedPartition.topic}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Partition Number</span>
                <span className="text-white font-bold">P{selectedPartition.partition}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Partition Leader</span>
                <span className="text-white font-bold">{selectedPartition.leader || 'NO_LEADER'}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Replica Broker Set</span>
                <span className="text-white">{selectedPartition.replicas.join(', ')}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">In-Sync Replicas (ISR)</span>
                <span className="text-white">{selectedPartition.isr.join(', ')}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Log End Offset</span>
                <span className="text-purple-400 font-bold">{selectedPartition.next_offset}</span>
              </div>

              <div>
                <span className="text-[10px] text-gray-500 uppercase block">Aggregated Consumer Lag</span>
                <span className={`font-bold ${selectedLag !== null && selectedLag > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                  {selectedLag !== null ? selectedLag : '0'}
                </span>
              </div>
            </div>
          </div>

          <div className="pt-4 border-t border-[#1a1c23] flex items-center space-x-2 text-gray-500 text-[10px]">
            <Activity className="h-3 w-3 text-purple-400 animate-pulse" />
            <span>Click any partition node to inspect.</span>
          </div>
        </div>
      )}
    </div>
  );
};
