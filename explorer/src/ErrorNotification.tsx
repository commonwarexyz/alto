import React, { useEffect } from 'react';

interface ErrorNotificationProps {
    message: string;
    isVisible: boolean;
    onDismiss: () => void;
    autoHideDuration?: number;
}

const ErrorNotification: React.FC<ErrorNotificationProps> = ({
    message,
    isVisible,
    onDismiss,
    autoHideDuration = 10000 // Default 10 seconds
}) => {
    useEffect(() => {
        if (isVisible && autoHideDuration > 0) {
            const timer = setTimeout(() => {
                onDismiss();
            }, autoHideDuration);

            return () => clearTimeout(timer);
        }
    }, [isVisible, autoHideDuration, onDismiss]);

    if (!isVisible) return null;

    return (
        <div className="error-notification">
            <div className="error-content">
                <span className="error-message">{message}</span>
                <button className="error-dismiss" onClick={onDismiss}>
                    ×
                </button>
            </div>
        </div>
    );
};

export default ErrorNotification;