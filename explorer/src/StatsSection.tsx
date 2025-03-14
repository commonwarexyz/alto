import React, { useState } from 'react';

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

    return (
        <div
            className="tooltip-container"
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => setIsVisible(false)}
            onClick={() => setIsVisible(!isVisible)}
        >
            {children}
            {isVisible && (
                <div className="tooltip-content">
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

    // Calculate averages
    const avgTimeToLock = notarizationTimes.length > 0
        ? Math.round(notarizationTimes.reduce((sum, time) => sum + time, 0) / notarizationTimes.length)
        : 0;

    const avgTimeToFinalize = finalizationTimes.length > 0
        ? Math.round(finalizationTimes.reduce((sum, time) => sum + time, 0) / finalizationTimes.length)
        : 0;

    const tooltips = {
        validators: "The total number of nodes participating in the consensus protocol.",
        timeToLock: "Average time (in milliseconds) from block proposal to notarization (2f+1 votes). Once a block is notarized, no conflicting block can be notarized in the same view.",
        timeToFinalize: "Average time (in milliseconds) from block proposal to finalization (2f+1 finalizes). Once finalized, the block is immutable and permanently part of the chain."
    };

    return (
        <div className="stats-section">
            <h2 className="stats-title">Metrics</h2>
            <div className="stats-container">
                <div className="stat-item">
                    <Tooltip content={tooltips.validators}>
                        <div className="stat-label">Validators</div>
                        <div className="stat-value">{numValidators}</div>
                    </Tooltip>
                </div>

                <div className="stat-item">
                    <Tooltip content={tooltips.timeToLock}>
                        <div className="stat-label">Time to Lock</div>
                        <div className="stat-value">
                            {avgTimeToLock > 0 ? `${avgTimeToLock}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>

                <div className="stat-item">
                    <Tooltip content={tooltips.timeToFinalize}>
                        <div className="stat-label">Time to Finalize</div>
                        <div className="stat-value">
                            {avgTimeToFinalize > 0 ? `${avgTimeToFinalize}ms` : "N/A"}
                        </div>
                    </Tooltip>
                </div>
            </div>
        </div>
    );
};

export default StatsSection;