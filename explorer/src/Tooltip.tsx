import React, { useState, useRef } from 'react';
import './Tooltip.css';

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
            onClick={() => setIsVisible(!isVisible)} // Toggle on click for mobile
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

export default Tooltip;