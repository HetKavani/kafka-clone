import { 
  Server, 
  Layers, 
  Users, 
  TrendingUp, 
  Database, 
  Clock 
} from 'lucide-react';
import { 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer, 
  BarChart, 
  Bar, 
  Legend 
} from 'recharts';
import type { Broker, Topic, PartitionMetadata, Metrics, ActivityLog, ConsumerGroupLag } from '../types';

interface DashboardProps {
  brokers: Broker[];
  topics: Topic[];
  partitions: PartitionMetadata[];
  groups: string[];
  groupLags: Record<string, ConsumerGroupLag>;
  metrics: Metrics | null;
  activity: ActivityLog[];
}

export const Dashboard: React.FC<DashboardProps> = ({
  brokers,
  topics,
  partitions,
  groups,
  groupLags,
  metrics,
  activity,
}) => {
  const activeBrokers = brokers.filter((b) => b.status === 'active');


  const totalLag = Object.values(groupLags).reduce((sum, gl) => sum + gl.total_lag, 0);

  // Prepare chart data using current metrics
  const chartData = [
    {
      name: 'Now',
      'Publish Latency (ms)': metrics?.avg_publish_latency_ms || 0,
      'Consume Latency (ms)': metrics?.avg_consume_latency_ms || 0,
    }
  ];

  const throughputData = [
    {
      name: 'Total Events',
      Published: metrics?.events_published || 0,
      Consumed: metrics?.events_consumed || 0,
    }
  ];

  return (
    <div className="space-y-6">
      {/* Metrics Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4">
        {/* Active Brokers */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <Server className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Active Brokers</div>
            <div className="text-lg font-bold font-mono text-white mt-0.5">
              {activeBrokers.length}
              <span className="text-xs text-gray-500 font-normal"> / {brokers.length}</span>
            </div>
          </div>
        </div>

        {/* Topics */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <Layers className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Topics</div>
            <div className="text-lg font-bold font-mono text-white mt-0.5">{topics.length}</div>
          </div>
        </div>

        {/* Partitions */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <Database className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Partitions</div>
            <div className="text-lg font-bold font-mono text-white mt-0.5">{partitions.length}</div>
          </div>
        </div>

        {/* Consumer Groups */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <Users className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Groups</div>
            <div className="text-lg font-bold font-mono text-white mt-0.5">{groups.length}</div>
          </div>
        </div>

        {/* Total Events */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <TrendingUp className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Events Pub/Sub</div>
            <div className="text-lg font-bold font-mono text-white mt-0.5">
              {metrics?.events_published || 0}
              <span className="text-xs text-gray-500 font-normal"> / {metrics?.events_consumed || 0}</span>
            </div>
          </div>
        </div>

        {/* Consumer Lag */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-4 rounded flex items-center space-x-4">
          <div className="p-2 bg-purple-950/20 border border-purple-800/20 rounded text-purple-400">
            <Clock className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xs text-gray-500 uppercase font-mono tracking-wider">Consumer Lag</div>
            <div className={`text-lg font-bold font-mono mt-0.5 ${totalLag > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
              {totalLag}
            </div>
          </div>
        </div>
      </div>

      {/* Main Grid: Lists and Graphs */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left: Brokers, Topics, Groups */}
        <div className="lg:col-span-2 space-y-6">
          {/* Charts Row */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-[#0c0d13] border border-[#1a1c23] p-5 rounded">
              <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400 mb-4">
                Latency Metrics (ms)
              </h3>
              <div className="h-44 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1f202c" />
                    <XAxis dataKey="name" stroke="#6b7280" fontSize={11} />
                    <YAxis stroke="#6b7280" fontSize={11} />
                    <Tooltip contentStyle={{ backgroundColor: '#11121c', borderColor: '#1f202c', color: '#fff' }} />
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                    <Bar dataKey="Publish Latency (ms)" fill="#a855f7" />
                    <Bar dataKey="Consume Latency (ms)" fill="#3b82f6" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="bg-[#0c0d13] border border-[#1a1c23] p-5 rounded">
              <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400 mb-4">
                Throughput Overview
              </h3>
              <div className="h-44 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={throughputData} margin={{ top: 5, right: 5, left: -20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1f202c" />
                    <XAxis dataKey="name" stroke="#6b7280" fontSize={11} />
                    <YAxis stroke="#6b7280" fontSize={11} />
                    <Tooltip contentStyle={{ backgroundColor: '#11121c', borderColor: '#1f202c', color: '#fff' }} />
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                    <Bar dataKey="Published" fill="#059669" />
                    <Bar dataKey="Consumed" fill="#0284c7" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Broker and Topic tables */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Active Brokers Overview */}
            <div className="bg-[#0c0d13] border border-[#1a1c23] rounded overflow-hidden">
              <div className="px-5 py-4 border-b border-[#1a1c23]">
                <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
                  Broker Registry
                </h3>
              </div>
              <div className="p-4">
                <table className="w-full text-left text-xs font-mono">
                  <thead>
                    <tr className="text-gray-500 border-b border-[#1a1c23] pb-2">
                      <th className="pb-2 font-normal">Broker ID</th>
                      <th className="pb-2 font-normal">Host</th>
                      <th className="pb-2 font-normal text-right">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {brokers.map((b) => (
                      <tr key={b.broker_id} className="hover:bg-[#12131a]/50">
                        <td className="py-2.5 text-white font-medium">{b.broker_id}</td>
                        <td className="py-2.5 text-gray-400">
                          {b.host ? `${b.host}:${b.port}` : 'N/A'}
                        </td>
                        <td className="py-2.5 text-right">
                          <span className={`inline-flex items-center space-x-1.5 px-2 py-0.5 rounded text-[10px] uppercase font-bold ${
                            b.status === 'active' 
                              ? 'bg-emerald-950/20 text-emerald-400 border border-emerald-800/30' 
                              : 'bg-red-950/20 text-red-400 border border-red-800/30'
                          }`}>
                            <span className={`h-1.5 w-1.5 rounded-full ${b.status === 'active' ? 'bg-emerald-500' : 'bg-red-500'}`} />
                            <span>{b.status}</span>
                          </span>
                        </td>
                      </tr>
                    ))}
                    {brokers.length === 0 && (
                      <tr>
                        <td colSpan={3} className="text-center py-6 text-gray-500">No registered brokers found</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Topics Overview */}
            <div className="bg-[#0c0d13] border border-[#1a1c23] rounded overflow-hidden">
              <div className="px-5 py-4 border-b border-[#1a1c23]">
                <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
                  Topic Inventory
                </h3>
              </div>
              <div className="p-4">
                <table className="w-full text-left text-xs font-mono">
                  <thead>
                    <tr className="text-gray-500 border-b border-[#1a1c23] pb-2">
                      <th className="pb-2 font-normal">Topic</th>
                      <th className="pb-2 font-normal text-center">Partitions</th>
                      <th className="pb-2 font-normal text-right">Replicas</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topics.map((t) => {
                      const topicParts = partitions.filter((p) => p.topic === t.name);
                      const isrHealthy = topicParts.every((p) => p.isr.length === p.replicas.length);

                      return (
                        <tr key={t.id} className="hover:bg-[#12131a]/50">
                          <td className="py-2.5 text-white font-medium">{t.name}</td>
                          <td className="py-2.5 text-center text-gray-400">{t.partition_count}</td>
                          <td className="py-2.5 text-right">
                            <span className={`inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] ${
                              isrHealthy 
                                ? 'bg-emerald-950/20 text-emerald-400 border border-emerald-800/30' 
                                : 'bg-amber-950/20 text-amber-400 border border-amber-800/30'
                            }`}>
                              <span>ISR</span>
                              <span className="font-bold">{isrHealthy ? 'HEALTHY' : 'DEGRADED'}</span>
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                    {topics.length === 0 && (
                      <tr>
                        <td colSpan={3} className="text-center py-6 text-gray-500">No active topics found</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        {/* Right: Live Activity Log */}
        <div className="bg-[#0c0d13] border border-[#1a1c23] rounded flex flex-col h-[520px]">
          <div className="px-5 py-4 border-b border-[#1a1c23] flex justify-between items-center flex-shrink-0">
            <h3 className="text-xs font-mono font-semibold uppercase tracking-widest text-gray-400">
              Live Activity Stream
            </h3>
            <span className="h-2 w-2 bg-purple-500 rounded-full animate-ping" />
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-3 font-mono text-[11px]">
            {activity.map((log) => {
              let textClass = 'text-gray-400';
              let badgeClass = 'text-gray-500 border-gray-800/40 bg-gray-950/30';
              
              if (log.type === 'publish') {
                textClass = 'text-purple-300';
                badgeClass = 'text-purple-400 border-purple-800/20 bg-purple-950/20';
              } else if (log.type === 'commit') {
                textClass = 'text-blue-300';
                badgeClass = 'text-blue-400 border-blue-800/20 bg-blue-950/20';
              } else if (log.type === 'election') {
                textClass = 'text-amber-300 font-bold';
                badgeClass = 'text-amber-400 border-amber-800/20 bg-amber-950/20';
              } else if (log.type === 'isr' || log.type === 'recovery') {
                textClass = 'text-emerald-300';
                badgeClass = 'text-emerald-400 border-emerald-800/20 bg-emerald-950/20';
              } else if (log.type === 'system') {
                textClass = 'text-red-300';
                badgeClass = 'text-red-400 border-red-800/20 bg-red-950/20';
              }

              return (
                <div key={log.id} className="border-b border-[#151722]/50 pb-2.5 last:border-0 flex items-start space-x-2">
                  <span className="text-gray-600 flex-shrink-0">{log.timestamp}</span>
                  <span className={`inline-block px-1 py-0.25 border rounded text-[9px] font-bold uppercase tracking-wider flex-shrink-0 ${badgeClass}`}>
                    {log.type}
                  </span>
                  <span className={`flex-1 break-words ${textClass}`}>{log.message}</span>
                </div>
              );
            })}
            {activity.length === 0 && (
              <div className="text-center py-20 text-gray-600 italic">Listening for cluster activities...</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
