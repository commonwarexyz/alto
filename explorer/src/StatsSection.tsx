import React, { useState, useRef } from 'react';

// ViewData interface needs to be imported by StatsSection
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
    numValidators: number;
}

interface TooltipProps {
    content: string;
    children: React.ReactNode;
}

const Tooltip: React.FC<TooltipProps> = ({ content, children }) => {
    const [isVisible, setIsVisible] = useState(false);
    const tooltipRef = useRef<HTMLDivElement>(null);

    return (
        <div
            className="tooltip-container"
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => setIsVisible(false)}
            onClick={() => setIsVisible(!isVisible)}
        >
            {children}
            {isVisible && (
                <div
                    className="tooltip-content"
                    ref={tooltipRef}
                >
                    {content}
                </div>
            )}
        </div>
    );
};

const StatsSection: React.FC<StatsSectionProps> = ({ views, numValidators }) => {
    // Calculate average time-to-lock (notarization latency)
    const notarizationTimes = views
        .filter(view => (view.status === "notarized" || view.status === "finalized"))
        .map(view => {
            // Use actualNotarizationLatency if available, otherwise calculate from timestamps
            if (view.actualNotarizationLatency !== undefined && view.actualNotarizationLatency > 0) {
                return view.actualNotarizationLatency;
            } else if (view.notarizationTime && view.startTime) {
                const calculatedLatency = view.notarizationTime - view.startTime;
                return calculatedLatency > 0 ? calculatedLatency : null;
            }
            return null;
        })
        .filter((time): time is number => time !== null);

    // Calculate average time-to-finalize
    const finalizationTimes = views
        .filter(view => view.status === "finalized")
        .map(view => {
            // Use actualFinalizationLatency if available, otherwise calculate from timestamps
            if (view.actualFinalizationLatency !== undefined && view.actualFinalizationLatency > 0) {
                return view.actualFinalizationLatency;
            } else if (view.finalizationTime && view.startTime) {
                const calculatedLatency = view.finalizationTime - view.startTime;
                return calculatedLatency > 0 ? calculatedLatency : null;
            }
            return null;
        })
        .filter((time): time is number => time !== null);

    // Calculate block times (time between consecutive blocks)
    const viewsWithBlocks = views
        .filter(view => view.block && view.block.height && view.block.timestamp)
        .sort((a, b) => a.block.height - b.block.height);

    const blockTimes: number[] = [];
    for (let i = 1; i < viewsWithBlocks.length; i++) {
        const currentBlock = viewsWithBlocks[i].block;
        const prevBlock = viewsWithBlocks[i - 1].block;

        if (currentBlock && prevBlock &&
            currentBlock.timestamp && prevBlock.timestamp &&
            currentBlock.height === prevBlock.height + 1) {

            const timeDiff = currentBlock.timestamp - prevBlock.timestamp;
            if (timeDiff > 0 && timeDiff < 10000) { // Filter out unreasonable values (>10s)
                blockTimes.push(timeDiff);
            }
        }
    }

    // Calculate median for blockTimes
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

    // Calculate median for notarizationTimes
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

    // Calculate median for finalizationTimes
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
        blockTime: "The median difference between consecutive block timestamps.",
        timeToLock: "The median latency from block proposal to receiving 2f+1 votes. Locked blocks must be included in the canonical chain if the view is not nullified.",
        timeToFinalize: "The median latency from block proposal to receiving 2f+1 finalizes. Once finalized, a block is immutable."
    };

    return (
        <div className="stats-section">
            <h2 className="stats-title">Summary</h2>
            <div className="stats-container">
                <div className="stat-item">
                    <Tooltip content={tooltips.blockTime}>
                        <div className="stat-label">Block Time</div>
                        <div className="stat-value">
                            {medianBlockTime > 0 ? `${medianBlockTime}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>

                <div className="stat-item">
                    <Tooltip content={tooltips.timeToLock}>
                        <div className="stat-label">Time-to-Lock (TTL)</div>
                        <div className="stat-value">
                            {medianTimeToLock > 0 ? `${medianTimeToLock}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>

                <div className="stat-item">
                    <Tooltip content={tooltips.timeToFinalize}>
                        <div className="stat-label">Time-to-Finalize (TTF)</div>
                        <div className="stat-value">
                            {medianTimeToFinalize > 0 ? `${medianTimeToFinalize}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>
            </div>
            <div className="stats-disclaimer">
                All latency measurements are made by your browser after verifying incoming consensus artifacts. If a validator's clock (or your local clock) is skewed, these values may appear incorrect.
            </div>
        </div>
    );
};

export default StatsSection;