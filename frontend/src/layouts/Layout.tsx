import React from 'react';
import { 
  LayoutDashboard, 
  Server, 
  Layers, 
  Users, 
  Search, 
  Network, 
  RefreshCw, 
  AlertTriangle 
} from 'lucide-react';
import type { ClusterSummary } from '../types';

interface LayoutProps {
  children: React.ReactNode;
  activeTab: string;
  setActiveTab: (tab: string) => void;
  summary: ClusterSummary | null;
  loading: boolean;
  error: string | null;
  onRefresh: () => void;
}

export const Layout: React.FC<LayoutProps> = ({
  children,
  activeTab,
  setActiveTab,
  summary,
  loading,
  error,
  onRefresh,
}) => {
  const navItems = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    { id: 'brokers', label: 'Brokers', icon: Server },
    { id: 'topics', label: 'Topics', icon: Layers },
    { id: 'consumers', label: 'Consumer Groups', icon: Users },
    { id: 'events', label: 'Events', icon: Search },
    { id: 'cluster', label: 'Cluster', icon: Network },
  ];

  // Determine cluster status style
  let statusText = 'Unknown';
  let statusColor = 'bg-gray-500';
  let statusTextColor = 'text-gray-400';

  if (error) {
    statusText = 'Unavailable';
    statusColor = 'bg-red-500 animate-pulse';
    statusTextColor = 'text-red-400';
  } else if (summary) {
    if (summary.status === 'healthy') {
      statusText = 'Healthy';
      statusColor = 'bg-emerald-500';
      statusTextColor = 'text-emerald-400';
    } else {
      statusText = 'Degraded';
      statusColor = 'bg-amber-500';
      statusTextColor = 'text-amber-400';
    }
  }

  return (
    <div className="flex h-screen bg-[#090a0f] text-[#d1d5db] font-sans antialiased overflow-hidden">
      {/* Sidebar */}
      <aside className="w-64 border-r border-[#1a1c23] bg-[#0c0d13] flex flex-col justify-between">
        <div>
          {/* Brand */}
          <div className="h-16 flex items-center px-6 border-b border-[#1a1c23]">
            <span className="font-mono text-lg font-bold tracking-wider text-purple-400">
              KAFKA<span className="text-white">X</span>_CONSOLE
            </span>
          </div>

          {/* Navigation */}
          <nav className="p-4 space-y-1">
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={`w-full flex items-center space-x-3 px-4 py-2.5 rounded text-sm transition-all duration-150 ${
                    isActive
                      ? 'bg-[#1e1f29] text-white border-l-2 border-purple-500 pl-3'
                      : 'text-gray-400 hover:text-white hover:bg-[#12131a]'
                  }`}
                >
                  <Icon className="h-4 w-4" />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>
        </div>

        {/* Status Indicator */}
        <div className="p-6 border-t border-[#1a1c23] bg-[#090a0f]">
          <div className="flex items-center space-x-2.5">
            <span className={`h-2.5 w-2.5 rounded-full ${statusColor}`} />
            <div className="flex flex-col">
              <span className="text-xs text-gray-500 uppercase tracking-widest">Cluster Status</span>
              <span className={`text-sm font-semibold font-mono ${statusTextColor}`}>{statusText}</span>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Container */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <header className="h-16 border-b border-[#1a1c23] bg-[#0c0d13] flex items-center justify-between px-8 z-10">
          <div className="flex items-center space-x-4">
            <h1 className="text-base font-semibold capitalize font-mono text-white tracking-wide">
              {activeTab.replace('-', ' ')}
            </h1>
          </div>

          <div className="flex items-center space-x-4">
            {loading && (
              <span className="text-xs font-mono text-purple-400 animate-pulse flex items-center space-x-1.5">
                <RefreshCw className="h-3 w-3 animate-spin" />
                <span>Syncing cluster...</span>
              </span>
            )}
            <button
              onClick={onRefresh}
              disabled={loading}
              className="p-2 text-gray-400 hover:text-white rounded border border-[#1a1c23] hover:bg-[#1e1f29] transition duration-150 disabled:opacity-50"
              title="Force Refresh Data"
            >
              <RefreshCw className="h-4 w-4" />
            </button>
          </div>
        </header>

        {/* Content Body */}
        <main className="flex-1 overflow-y-auto bg-[#090a0f] p-8">
          {error && (
            <div className="mb-6 p-4 bg-red-950/20 border border-red-800/40 rounded text-red-200 flex items-start space-x-3 max-w-4xl">
              <AlertTriangle className="h-5 w-5 text-red-500 mt-0.5 flex-shrink-0" />
              <div className="flex-1">
                <h4 className="font-semibold text-sm">Cluster Connectivity Failure</h4>
                <p className="text-xs text-red-400/80 mt-1">{error}</p>
                <button
                  onClick={onRefresh}
                  className="mt-3 px-3 py-1.5 bg-red-900/40 hover:bg-red-900/60 border border-red-700/50 rounded text-xs font-mono transition duration-150"
                >
                  Retry Connection
                </button>
              </div>
            </div>
          )}

          {children}
        </main>
      </div>
    </div>
  );
};
