import * as globalConfig from './global_config';
import * as usaConfig from './usa_config';
import * as localConfig from './local_config';

export type Cluster = 'global' | 'usa' | 'local';
export type Mode = 'public' | 'local';

export interface ClusterConfig {
    BACKEND_URL: string;
    PUBLIC_KEY_HEX: string;
    LOCATIONS: [[number, number], string][];
    name: string;
    description: string;
}

// Detect mode from environment variable (set at build time)
// Usage: REACT_APP_MODE=local npm start
// Default to 'public' if not set
export const MODE: Mode = (process.env.REACT_APP_MODE as Mode) || 'public';

// Build configs based on mode
const publicConfigs: Record<'global' | 'usa', ClusterConfig> = {
    global: {
        ...globalConfig,
        name: 'Global Cluster',
        description: `A cluster of <strong>50 validators</strong> running c8g.large (2 vCPU, 4GB RAM) nodes on AWS in <strong>10 regions</strong> (us-west-1, us-east-1, eu-west-1, ap-northeast-1, eu-north-1, ap-south-1, sa-east-1, eu-central-1, ap-northeast-2, ap-southeast-2).`,
    },
    usa: {
        ...usaConfig,
        name: 'USA Cluster',
        description: `A cluster of <strong>50 validators</strong> running c8g.large (2 vCPU, 4GB RAM) nodes on AWS in <strong>4 regions</strong> (us-east-1, us-west-1, us-east-2, us-west-2).`,
    },
};

const localClusterConfig: ClusterConfig = {
    ...localConfig,
    name: 'Local Cluster',
    description: `A local test cluster running on localhost.`,
};

export const DEFAULT_CLUSTER: Cluster = MODE === 'public' ? 'global' : 'local';

export const getClusterConfig = (cluster: Cluster): ClusterConfig => {
    if (MODE === 'local') {
        return localClusterConfig;
    }
    if (cluster === 'local') {
        return localClusterConfig;
    }
    return publicConfigs[cluster];
};

export const getClusters = (): Record<Cluster, ClusterConfig> => {
    if (MODE === 'local') {
        return { local: localClusterConfig } as Record<Cluster, ClusterConfig>;
    }
    return publicConfigs as Record<Cluster, ClusterConfig>;
};
