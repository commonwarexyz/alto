import React, { useState, useRef, useEffect } from 'react';
import { Cluster, ClusterConfig, MODE } from './config';

// ViewData interface (no changes)
export interface ViewData {
    view: number;
    location?: [number, number];
    locationName?: string;
    status: "growing" | "notarized" | "finalized" | "timed_out" | "unknown";
    startTime: number;
    notarizationTime?: number;
    finalizationTime?: number;
    signature?: Uint8Array;
    block?: any; // BlockJs
    timeoutId?: any; // NodeJS.Timeout
    actualNotarizationLatency?: number;
    actualFinalizationLatency?: number;
}

interface StatsSectionProps {
    views: ViewData[];
    selectedCluster: Cluster;
    onClusterChange: (cluster: Cluster) => void;
    configs: Record<Cluster, ClusterConfig>;
}

interface DropdownProps {
    selectedCluster: Cluster;
    onClusterChange: (cluster: Cluster) => void;
    configs: Record<Cluster, ClusterConfig>;
}

const Dropdown: React.FC<DropdownProps> = ({ selectedCluster, onClusterChange, configs }) => {
    const [isOpen, setIsOpen] = useState(false);
    const dropdownRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, []);

    const handleSelect = (cluster: Cluster) => {
        onClusterChange(cluster);
        setIsOpen(false);
    };

    return (
        <div className="custom-dropdown" ref={dropdownRef}>
            <button
                className={`dropdown-trigger ${selectedCluster === 'global' ? 'global-cluster' : 'usa-cluster'}`}
                onClick={() => setIsOpen(!isOpen)}
                aria-expanded={isOpen}
                aria-haspopup="listbox"
            >
                <span className="dropdown-label">{configs[selectedCluster].name}</span>
                <span className="dropdown-arrow">{isOpen ? '▲' : '▼'}</span>
            </button>

            {isOpen && (
                <div className="dropdown-menu">
                    {Object.entries(configs).map(([clusterId, config]) => (
                        <button
                            key={clusterId}
                            className={`dropdown-option ${selectedCluster === clusterId ? 'selected' : ''} ${clusterId === 'global' ? 'global-option' : 'usa-option'}`}
                            onClick={() => handleSelect(clusterId as Cluster)}
                        >
                            <div className="option-name">{config.name}</div>
                            <div className="option-description" dangerouslySetInnerHTML={{ __html: config.description }} />
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
};

interface TooltipProps {
    content: string;
    children: React.ReactNode;
}

const Tooltip: React.FC<TooltipProps> = ({ content, children }) => {
    const [isVisible, setIsVisible] = useState(false);
    const tooltipRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);

    // Handle clicks outside the tooltip to close it
    useEffect(() => {
        if (!isVisible) return;
        const handleOutsideClick = (event: MouseEvent | TouchEvent) => {
            if (
                containerRef.current &&
                !containerRef.current.contains(event.target as Node)
            ) {
                setIsVisible(false);
            }
        };

        document.addEventListener('mousedown', handleOutsideClick);
        document.addEventListener('touchstart', handleOutsideClick);
        return () => {
            document.removeEventListener('mousedown', handleOutsideClick);
            document.removeEventListener('touchstart', handleOutsideClick);
        };
    }, [isVisible]);

    // Separate handlers for desktop and mobile
    const handleDesktopInteraction = () => {
        if (window.matchMedia('(hover: hover)').matches) {
            return {
                onMouseEnter: () => setIsVisible(true),
                onMouseLeave: () => setIsVisible(false)
            };
        }
        return {};
    };

    const handleClick = (e: React.MouseEvent | React.TouchEvent) => {
        e.stopPropagation();
        setIsVisible(!isVisible);
    };

    return (
        <div
            className="tooltip-container"
            ref={containerRef}
            onClick={handleClick}
            onTouchEnd={(e) => {
                e.preventDefault();
                handleClick(e);
            }}
            {...handleDesktopInteraction()}
        >
            {children}
            {isVisible && (
                <div
                    className="tooltip-content"
                    ref={tooltipRef}
                    onClick={(e) => e.stopPropagation()}
                    dangerouslySetInnerHTML={{ __html: content }}>
                </div>
            )}
        </div>
    );
};

const StatsSection: React.FC<StatsSectionProps> = ({ views, selectedCluster, onClusterChange, configs }) => {
    // Calculation logic (unchanged from original)
    const notarizationTimes = views
        .filter(view => (view.status === "notarized" || view.status === "finalized"))
        .map(view => {
            if (view.actualNotarizationLatency !== undefined && view.actualNotarizationLatency > 0) {
                return view.actualNotarizationLatency;
            } else if (view.notarizationTime && view.startTime) {
                const calculatedLatency = view.notarizationTime - view.startTime;
                return calculatedLatency > 0 ? calculatedLatency : null;
            }
            return null;
        })
        .filter((time): time is number => time !== null);

    const finalizationTimes = views
        .filter(view => view.status === "finalized")
        .map(view => {
            if (view.actualFinalizationLatency !== undefined && view.actualFinalizationLatency > 0) {
                return view.actualFinalizationLatency;
            } else if (view.finalizationTime && view.startTime) {
                const calculatedLatency = view.finalizationTime - view.startTime;
                return calculatedLatency > 0 ? calculatedLatency : null;
            }
            return null;
        })
        .filter((time): time is number => time !== null);

    const viewsWithBlocks = views
        .filter(view => view.block && view.block.height && view.block.timestamp)
        .sort((a, b) => a.block.height - b.block.height);

    const blockTimes: number[] = [];

    // Group consecutive blocks to ensure we're only comparing truly adjacent blocks
    const consecutiveGroups: typeof viewsWithBlocks[] = [];
    let currentGroup: typeof viewsWithBlocks = [viewsWithBlocks[0]];

    for (let i = 1; i < viewsWithBlocks.length; i++) {
        const currentBlock = viewsWithBlocks[i].block;
        const prevBlock = currentGroup[currentGroup.length - 1].block;

        // If this block is consecutive to the last one in current group, add it
        if (currentBlock.height === prevBlock.height + 1) {
            currentGroup.push(viewsWithBlocks[i]);
        } else {
            // Start a new group if we have a gap
            if (currentGroup.length >= 2) {
                consecutiveGroups.push(currentGroup);
            }
            currentGroup = [viewsWithBlocks[i]];
        }
    }

    // Don't forget the last group
    if (currentGroup.length >= 2) {
        consecutiveGroups.push(currentGroup);
    }

    // Calculate block times only within consecutive groups
    for (const group of consecutiveGroups) {
        // Only process groups with at least 3 blocks for more reliable measurements
        if (group.length >= 3) {
            for (let i = 1; i < group.length; i++) {
                const currentBlock = group[i].block;
                const prevBlock = group[i - 1].block;

                // Use raw block timestamps directly - DO NOT apply clock skew correction
                // Block timestamps come from the network itself, not browser time
                const timeDiff = currentBlock.timestamp - prevBlock.timestamp;
                blockTimes.push(timeDiff);
            }
        }
    }

    // Calculate medians
    const sortedBlockTimes = [...blockTimes].sort((a, b) => a - b);
    const medianBlockTime =
        sortedBlockTimes.length > 0
            ? sortedBlockTimes.length % 2 === 1
                ? sortedBlockTimes[Math.floor(sortedBlockTimes.length / 2)]
                : Math.round(
                    (sortedBlockTimes[sortedBlockTimes.length / 2 - 1] +
                        sortedBlockTimes[sortedBlockTimes.length / 2]) /
                    2
                )
            : 0;

    const sortedNotarizationTimes = [...notarizationTimes].sort((a, b) => a - b);
    const medianTimeToLock =
        sortedNotarizationTimes.length > 0
            ? sortedNotarizationTimes.length % 2 === 1
                ? sortedNotarizationTimes[Math.floor(sortedNotarizationTimes.length / 2)]
                : Math.round(
                    (sortedNotarizationTimes[sortedNotarizationTimes.length / 2 - 1] +
                        sortedNotarizationTimes[sortedNotarizationTimes.length / 2]) /
                    2
                )
            : 0;

    const sortedFinalizationTimes = [...finalizationTimes].sort((a, b) => a - b);
    const medianTimeToFinalize =
        sortedFinalizationTimes.length > 0
            ? sortedFinalizationTimes.length % 2 === 1
                ? sortedFinalizationTimes[Math.floor(sortedFinalizationTimes.length / 2)]
                : Math.round(
                    (sortedFinalizationTimes[sortedFinalizationTimes.length / 2 - 1] +
                        sortedFinalizationTimes[sortedFinalizationTimes.length / 2]) /
                    2
                )
            : 0;

    const tooltips = {
        blockTime: "The median difference between consecutive block timestamps.<br><br><i>This is functionally equivalent to the average validator's time to lock (unlike your browser, validators are connected directly to each other instead of an intermediary streaming layer).</i>",
        timeToLock: "The median latency from block proposal to receiving 2f+1 votes, as observed by your browser.<br><br><i>Locked blocks must be included in the canonical chain if the view is not nullified.</i>",
        timeToFinalize: "The median latency from block proposal to receiving 2f+1 finalizes, as observed by your browser.<br><br><i>Once finalized, a block is immutable.</i>"
    };

    return (
        <div className="stats-card">
            <div className="stats-header">
                <h2 className="stats-title">Latency</h2>
                {MODE === 'public' && (
                    <div className="cluster-toggle">
                        <Dropdown
                            selectedCluster={selectedCluster}
                            onClusterChange={onClusterChange}
                            configs={configs}
                        />
                    </div>
                )}
            </div>

            <div className="stats-grid">
                <div className="stat-box validator-metrics">
                    <div className="source-label">CLUSTER</div>
                    <Tooltip content={tooltips.blockTime}>
                        <div className="metric-container">
                            <div className="stat-label">Block Time</div>
                            <div className="stat-value">
                                {medianBlockTime > 0 ? `${medianBlockTime}ms` : "N/A"}
                            </div>
                        </div>
                    </Tooltip>
                </div>

                <div className="stat-box browser-metrics">
                    <div className="source-label">BROWSER</div>
                    <div className="browser-metrics-container">
                        <div className="metric-container">
                            <Tooltip content={tooltips.timeToLock}>
                                <div className="stat-label">Locked</div>
                                <div className="stat-value">
                                    {medianTimeToLock > 0 ? `${medianTimeToLock}ms` : "N/A"}
                                </div>
                            </Tooltip>
                        </div>

                        <div className="metric-container">
                            <Tooltip content={tooltips.timeToFinalize}>
                                <div className="stat-label">Finalized</div>
                                <div className="stat-value">
                                    {medianTimeToFinalize > 0 ? `${medianTimeToFinalize}ms` : "N/A"}
                                </div>
                            </Tooltip>
                        </div>
                    </div>
                </div>
            </div>

            <div className="stats-disclaimer">
                All latency measurements made by your browser are only performed after verifying the integrity of incoming artifacts with the network key.
                Local clock skew is automatically detected and corrected.
            </div>
        </div >
    );
};

export default StatsSection;