import React from 'react';
import { useMap } from 'react-leaflet';
import './MapOverlay.css';

interface MapOverlayProps {
    numValidators: number;
}

const MapOverlay: React.FC<MapOverlayProps> = ({ numValidators }) => {
    // We use useMap hook to ensure the component is rendered inside a MapContainer
    useMap();

    return (
        <div className="map-overlay">
            <div className="map-overlay-content">
                <div className="overlay-item">
                    <div className="overlay-label">Validators:</div>
                    <div className="overlay-value">{numValidators}</div>
                </div>
            </div>
        </div>
    );
};

export default MapOverlay;