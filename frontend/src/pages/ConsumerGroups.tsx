import React, { useState } from 'react';
import { Users, Eye } from 'lucide-react';
import type { ConsumerGroupLag } from '../types';

interface ConsumerGroupsProps {
  groups: string[];
  groupLags: Record<string, ConsumerGroupLag>;
}

export const ConsumerGroups: React.FC<ConsumerGroupsProps> = ({ groups, groupLags }) => {
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null);

  const getStatus = (g: string) => {
    const lagData = groupLags[g];
    if (lagData && lagData.lag_details.length > 0) {
      return 'active';
    }
    return 'inactive';
  };

  const selectedLagData = selectedGroup ? groupLags[selectedGroup] : null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Consumer Groups List */}
      <div className={`lg:col-span-2 bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col justify-between overflow-hidden ${selectedGroup ? 'lg:col-span-2' : 'lg:col-span-3'}`}>
        <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30">
          <div className="flex items-center space-x-2">
            <Users className="h-4 w-4 text-purple-400" />
            <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
              Consumer Groups
            </h3>
          </div>
        </div>

        <div className="p-6">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="text-gray-500 border-b border-[#1a1c23] pb-2">
                <th className="pb-2 font-normal">Group ID</th>
                <th className="pb-2 font-normal text-center">Partitions Watched</th>
                <th className="pb-2 font-normal text-center">Total Lag</th>
                <th className="pb-2 font-normal">Status</th>
                <th className="pb-2 font-normal text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => {
                const lagData = groupLags[g];
                const partsCount = lagData?.lag_details.length || 0;
                const totalLag = lagData?.total_lag || 0;
                const status = getStatus(g);

                return (
                  <tr key={g} className="hover:bg-[#12131a]/50">
                    <td className="py-3 text-white font-medium">
                      <button
                        onClick={() => setSelectedGroup(g)}
                        className="hover:underline text-left font-bold"
                      >
                        {g}
                      </button>
                    </td>
                    <td className="py-3 text-center text-gray-400">{partsCount}</td>
                    <td className={`py-3 text-center font-bold ${totalLag > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                      {totalLag}
                    </td>
                    <td className="py-3">
                      <span className={`inline-flex items-center space-x-1.5 px-2 py-0.5 rounded text-[10px] uppercase font-bold ${
                        status === 'active' 
                          ? 'bg-emerald-950/20 text-emerald-400 border border-emerald-800/30' 
                          : 'bg-gray-950/20 text-gray-400 border border-gray-800/30'
                      }`}>
                        <span className={`h-1.5 w-1.5 rounded-full ${status === 'active' ? 'bg-emerald-500' : 'bg-gray-500'}`} />
                        <span>{status}</span>
                      </span>
                    </td>
                    <td className="py-3 text-right">
                      <button
                        onClick={() => setSelectedGroup(g)}
                        className="px-2.5 py-1 bg-[#1e1f29] hover:bg-[#282936] text-gray-300 rounded border border-[#2b2d3d] transition duration-150"
                        title="Inspect Group Details"
                      >
                        <Eye className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  </tr>
                );
              })}
              {groups.length === 0 && (
                <tr>
                  <td colSpan={5} className="text-center py-10 text-gray-500">No consumer groups detected in the cluster.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Selected Group Offset & Lag Details */}
      {selectedGroup && selectedLagData && (
        <div className="bg-[#0c0d13] border border-[#1a1c23] rounded overflow-hidden flex flex-col justify-between">
          <div className="px-6 py-4 border-b border-[#1a1c23] bg-[#0c0d13]/30 flex justify-between items-center">
            <div>
              <span className="text-[10px] text-gray-500 font-mono uppercase tracking-widest block">Group Partition Lag</span>
              <span className="text-sm font-bold text-white font-mono">{selectedGroup}</span>
            </div>
            <button
              onClick={() => setSelectedGroup(null)}
              className="text-gray-400 hover:text-white font-mono text-xs"
            >
              [ Close ]
            </button>
          </div>

          <div className="p-6 flex-1 space-y-4 overflow-y-auto">
            {selectedLagData.lag_details.map((detail) => (
              <div key={`${detail.topic}-${detail.partition}`} className="p-4 bg-[#12131a] border border-[#1a1c23] rounded space-y-3 font-mono text-xs">
                <div className="flex justify-between items-center border-b border-[#1a1c23] pb-2">
                  <span className="font-bold text-white">{detail.topic}/P{detail.partition}</span>
                  <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold ${
                    detail.lag > 0 ? 'bg-amber-950/20 text-amber-400' : 'bg-emerald-950/20 text-emerald-400'
                  }`}>
                    Lag: {detail.lag}
                  </span>
                </div>

                <div className="space-y-1.5 text-gray-400">
                  <div className="flex justify-between">
                    <span>Log End Offset:</span>
                    <span className="text-white font-bold">{detail.latest_offset}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Committed Offset:</span>
                    <span className="text-purple-400 font-bold">{detail.committed_offset}</span>
                  </div>
                </div>
              </div>
            ))}
            
            {selectedLagData.lag_details.length === 0 && (
              <div className="text-center py-10 text-gray-500 italic">No assigned partitions found for this group.</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
