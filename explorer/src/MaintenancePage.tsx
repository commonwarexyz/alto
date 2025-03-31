import React from 'react';
import './MaintenancePage.css';

const MaintenancePage: React.FC = () => {
    return (
        <div className="maintenance-container">
            <div className="maintenance-content">
                <div className="logo-container">
                    <div className="logo-line">
                        <span className="edge-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol">~</span>
                        <span className="horizontal-logo-symbol"> </span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol"> </span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">~</span>
                        <span className="horizontal-logo-symbol">~</span>
                        <span className="edge-logo-symbol">*</span>
                    </div>
                    <div className="logo-line">
                        <span className="vertical-logo-symbol">|</span>
                        <span className="logo-text"> commonware </span>
                        <span className="vertical-logo-symbol"> </span>
                    </div>
                    <div className="logo-line">
                        <span className="edge-logo-symbol">*</span>
                        <span className="horizontal-logo-symbol">~</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol"> </span>
                        <span className="horizontal-logo-symbol">~</span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">+</span>
                        <span className="horizontal-logo-symbol"> </span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="horizontal-logo-symbol">*</span>
                        <span className="horizontal-logo-symbol">-</span>
                        <span className="edge-logo-symbol">+</span>
                    </div>
                </div>

                <h1>System Maintenance</h1>
                <p>The alto explorer is currently undergoing maintenance.</p>
                <p>Please check back later.</p>

                <div className="maintenance-links">
                    <a href="https://github.com/commonwarexyz/alto" target="_blank" rel="noopener noreferrer">
                        <span className="link-icon">⌘</span> GitHub Repository
                    </a>
                    <a href="https://x.com/commonwarexyz" target="_blank" rel="noopener noreferrer">
                        <span className="link-icon">✕</span> Twitter
                    </a>
                </div>
            </div>
        </div>
    );
};

export default MaintenancePage;