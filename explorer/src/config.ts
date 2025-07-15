import * as globalConfig from './global_config';
import * as usaConfig from './usa_config';

export type Cluster = 'global' | 'usa';

export interface ClusterConfig {
    BACKEND_URL: string;
    PUBLIC_KEY_HEX: string;
    LOCATIONS: [[number, number], string][];
    name: string;
    description: string;
}

const configs: Record<Cluster, ClusterConfig> = {
    global: {
        ...globalConfig,
        name: 'Global Cluster',
        description: `A cluster of <strong>50 validators</strong> running c7g.large (2 vCPU, 4GB RAM) nodes on AWS in <strong>10 regions</strong> (us-west-1, us-east-1, eu-west-1, ap-northeast-1, eu-north-1, ap-south-1, sa-east-1, eu-central-1, ap-northeast-2, ap-southeast-2).`,
    },
    usa: {
        ...usaConfig,
        name: 'USA Cluster',
        description: `A cluster of <strong>50 validators</strong> running c7g.large (2 vCPU, 4GB RAM) nodes on AWS in <strong>4 regions</strong> (us-east-1, us-west-1, us-east-2, us-west-2).`,
    }
};

export const getClusterConfig = (cluster: Cluster): ClusterConfig => {
    return configs[cluster];
};

export const getClusters = (): Record<Cluster, ClusterConfig> => {
    return configs;
};