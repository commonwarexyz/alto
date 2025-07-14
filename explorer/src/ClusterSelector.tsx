import React from 'react';
import { Cluster, ClusterConfig } from './config';
import './ClusterSelector.css';

interface ClusterSelectorProps {
    selectedCluster: Cluster;
    onClusterChange: (cluster: Cluster) => void;
    configs: Record<Cluster, ClusterConfig>;
}

const ClusterSelector: React.FC<ClusterSelectorProps> = ({ selectedCluster, onClusterChange, configs }) => {
    return (
        <div className="cluster-selector-card">
            <div className="cluster-options">
                {Object.keys(configs).map((clusterId) => (
                    <button
                        key={clusterId}
                        className={`cluster-option ${selectedCluster === clusterId ? 'selected' : ''}`}
                        onClick={() => onClusterChange(clusterId as Cluster)}
                    >
                        {configs[clusterId as Cluster].name}
                    </button>
                ))}
            </div>
            <div className="cluster-description" dangerouslySetInnerHTML={{ __html: configs[selectedCluster].description }}>
            </div>
        </div>
    );
};

export default ClusterSelector;