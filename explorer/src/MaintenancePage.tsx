import React from 'react';
import './MaintenancePage.css';

const MaintenancePage: React.FC = () => {
    return (
        <div className="maintenance-container">
            <div className="maintenance-content">
                <h1>System Maintenance</h1>
                <p>The alto explorer is currently undergoing maintenance.</p>
                <p>Please check back later.</p>

                <div className="maintenance-links">
                    <a href="https://github.com/commonwarexyz/alto" target="_blank" rel="noopener noreferrer">
                        GitHub
                    </a>
                </div>
            </div>
        </div>
    );
};

export default MaintenancePage;