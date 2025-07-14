import React from 'react';
import { useMap } from 'react-leaflet';
import './ConnectionStatusOverlay.css';

interface ConnectionStatusOverlayProps {
    connectionError?: boolean;
    connectionStatusKnown?: boolean;
}

const ConnectionStatusOverlay: React.FC<ConnectionStatusOverlayProps> = ({
    connectionError = false,
    connectionStatusKnown = false
}) => {
    // We use useMap hook to ensure the component is rendered inside a MapContainer
    useMap();

    if (!connectionStatusKnown) return null;

    return (
        <div className="connection-status-overlay">
            <div className={`connection-status-badge ${connectionError ? 'error' : 'success'}`}>
                <span className={`connection-status-dot ${connectionError ? 'error' : 'success'}`}></span>
                {connectionError ? 'DISCONNECTED' : 'CONNECTED'}
            </div>
        </div>
    );
};

export default ConnectionStatusOverlay;