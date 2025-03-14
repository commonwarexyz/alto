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
        blockTime: "The median time between consecutive blocks. This represents how quickly the blockchain is producing new blocks.",
        timeToLock: "The median time (in milliseconds) from block proposal to notarization (2f+1 votes). Once a block is notarized, no conflicting block can be notarized in the same view.",
        timeToFinalize: "The median time (in milliseconds) from block proposal to finalization (2f+1 finalizes). Once finalized, the block is immutable."
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
                        <div className="stat-label">Time to Prepare</div>
                        <div className="stat-value">
                            {medianTimeToLock > 0 ? `${medianTimeToLock}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>

                <div className="stat-item">
                    <Tooltip content={tooltips.timeToFinalize}>
                        <div className="stat-label">Time to Finalize</div>
                        <div className="stat-value">
                            {medianTimeToFinalize > 0 ? `${medianTimeToFinalize}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>
            </div>
            <div className="stats-disclaimer">
                Latency (the delta between a block timestamp and your local clock) is recorded by your browser after verifying each incoming consensus artifact.
            </div>
        </div>
    );
};

export default StatsSection;