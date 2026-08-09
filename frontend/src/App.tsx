import { useState } from 'react';
import { useClusterData } from './hooks/useClusterData';
import { Layout } from './layouts/Layout';
import { Dashboard } from './pages/Dashboard';
import { Brokers } from './pages/Brokers';
import { Topics } from './pages/Topics';
import { ConsumerGroups } from './pages/ConsumerGroups';
import { Events } from './pages/Events';
import { Topology } from './pages/Topology';
import { ServerCrash, RefreshCw } from 'lucide-react';

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const {
    summary,
    brokers,
    partitions,
    topics,
    groups,
    groupLags,
    metrics,
    activity,
    loading,
    error,
    refresh,
  } = useClusterData();

  // If backend is completely offline at initial load
  if (loading && error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-[#090a0f] text-[#d1d5db]">
        <div className="bg-[#0c0d13] border border-[#1a1c23] p-8 rounded max-w-md w-full text-center space-y-6 shadow-2xl">
          <ServerCrash className="h-12 w-12 text-red-500 mx-auto animate-pulse" />
          <div className="space-y-2">
            <h2 className="text-sm font-mono font-bold uppercase tracking-widest text-white">
              Backend Unavailable
            </h2>
            <p className="text-xs text-gray-500 font-mono leading-relaxed">
              Unable to establish connection to the KafkaX event streaming API gateway. Verify the backend containers are running.
            </p>
          </div>
          <button
            onClick={refresh}
            className="w-full py-2 bg-purple-600 hover:bg-purple-700 text-white rounded font-mono font-bold text-xs flex items-center justify-center space-x-1.5 transition duration-150"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            <span>Reconnect Cluster</span>
          </button>
        </div>
      </div>
    );
  }

  const renderContent = () => {
    switch (activeTab) {
      case 'dashboard':
        return (
          <Dashboard
            brokers={brokers}
            topics={topics}
            partitions={partitions}
            groups={groups}
            groupLags={groupLags}
            metrics={metrics}
            activity={activity}
          />
        );
      case 'brokers':
        return <Brokers brokers={brokers} partitions={partitions} loading={loading} />;
      case 'topics':
        return <Topics topics={topics} partitions={partitions} onRefresh={refresh} />;
      case 'consumers':
        return <ConsumerGroups groups={groups} groupLags={groupLags} />;
      case 'events':
        return <Events topics={topics} partitions={partitions} onRefresh={refresh} />;
      case 'cluster':
        return <Topology brokers={brokers} partitions={partitions} groupLags={groupLags} />;
      default:
        return (
          <div className="text-center py-20 font-mono text-xs text-gray-500">
            Console Page In Development
          </div>
        );
    }
  };

  return (
    <Layout
      activeTab={activeTab}
      setActiveTab={setActiveTab}
      summary={summary}
      loading={loading}
      error={error}
      onRefresh={refresh}
    >
      {renderContent()}
    </Layout>
  );
}

export default App;
