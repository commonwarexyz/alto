import React from 'react';

interface KeyInfoButtonProps {
    onClick: () => void;
}

const KeyInfoButton: React.FC<KeyInfoButtonProps> = ({ onClick }) => {
    return (
        <button
            className="key-header-button"
            onClick={onClick}
            title="View Network Key Information"
        >
            <span style={{ fontSize: '16px' }}>⚷</span>
        </button>
    );
};

export default KeyInfoButton;