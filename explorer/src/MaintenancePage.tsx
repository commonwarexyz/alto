import React, { useState, useEffect, useRef } from 'react';
import './MaintenancePage.css';

const MaintenancePage: React.FC = () => {
    const containerRef = useRef<HTMLDivElement>(null);
    const logoRef = useRef<HTMLDivElement>(null);
    const positionRef = useRef({ x: 50, y: 50 });
    const directionRef = useRef({ x: 1, y: 1 });
    const [color, setColor] = useState('#0000ee');
    const animationFrameRef = useRef<number | null>(null);
    const initializedRef = useRef(false);

    // Speed in pixels per frame
    const speed = 1.5;

    // Array of vibrant colors for the bouncing logo
    const colors = [
        '#0000ee', '#ee0000', '#00ee00', '#ee00ee',
        '#eeee00', '#00eeee', '#ff7700', '#7700ff'
    ];

    // Get a random color that's different from the current one
    const getRandomColor = () => {
        const filteredColors = colors.filter(c => c !== color);
        return filteredColors[Math.floor(Math.random() * filteredColors.length)];
    };

    useEffect(() => {
        // Initialize position if needed
        if (!initializedRef.current && containerRef.current && logoRef.current) {
            const containerWidth = containerRef.current.clientWidth;
            const containerHeight = containerRef.current.clientHeight;
            const logoWidth = logoRef.current.clientWidth;
            const logoHeight = logoRef.current.clientHeight;

            // Set initial position
            positionRef.current = {
                x: Math.random() * (containerWidth - logoWidth),
                y: Math.random() * (containerHeight - logoHeight)
            };

            initializedRef.current = true;

            // Force a re-render to show initial position
            logoRef.current.style.left = `${positionRef.current.x}px`;
            logoRef.current.style.top = `${positionRef.current.y}px`;
        }

        // Animation function that doesn't depend on React state for positioning
        const animate = () => {
            if (!containerRef.current || !logoRef.current) {
                animationFrameRef.current = requestAnimationFrame(animate);
                return;
            }

            const containerWidth = containerRef.current.clientWidth;
            const containerHeight = containerRef.current.clientHeight;
            const logoWidth = logoRef.current.clientWidth;
            const logoHeight = logoRef.current.clientHeight;

            // Update position based on current direction
            let newX = positionRef.current.x + speed * directionRef.current.x;
            let newY = positionRef.current.y + speed * directionRef.current.y;
            let colorChanged = false;

            // Handle horizontal boundaries
            if (newX <= 0) {
                // Hit left edge
                directionRef.current.x = Math.abs(directionRef.current.x); // Ensure positive
                newX = 0; // Stop at boundary
                if (!colorChanged) {
                    setColor(getRandomColor());
                    colorChanged = true;
                }
            } else if (newX + logoWidth >= containerWidth) {
                // Hit right edge
                directionRef.current.x = -Math.abs(directionRef.current.x); // Ensure negative
                newX = containerWidth - logoWidth; // Stop at boundary
                if (!colorChanged) {
                    setColor(getRandomColor());
                    colorChanged = true;
                }
            }

            // Handle vertical boundaries
            if (newY <= 0) {
                // Hit top edge
                directionRef.current.y = Math.abs(directionRef.current.y); // Ensure positive
                newY = 0; // Stop at boundary
                if (!colorChanged) {
                    setColor(getRandomColor());
                    colorChanged = true;
                }
            } else if (newY + logoHeight >= containerHeight) {
                // Hit bottom edge
                directionRef.current.y = -Math.abs(directionRef.current.y); // Ensure negative
                newY = containerHeight - logoHeight; // Stop at boundary
                if (!colorChanged) {
                    setColor(getRandomColor());
                    colorChanged = true;
                }
            }

            // Update position reference
            positionRef.current = { x: newX, y: newY };

            // Apply the position directly to the DOM element
            logoRef.current.style.left = `${newX}px`;
            logoRef.current.style.top = `${newY}px`;

            // Continue animation
            animationFrameRef.current = requestAnimationFrame(animate);
        };

        // Start animation
        animationFrameRef.current = requestAnimationFrame(animate);

        // Clean up
        return () => {
            if (animationFrameRef.current !== null) {
                cancelAnimationFrame(animationFrameRef.current);
            }
        };
    }, []); // Run once on mount

    // Handle window resize to keep logo in bounds
    useEffect(() => {
        const handleResize = () => {
            if (containerRef.current && logoRef.current) {
                const containerWidth = containerRef.current.clientWidth;
                const containerHeight = containerRef.current.clientHeight;
                const logoWidth = logoRef.current.clientWidth;
                const logoHeight = logoRef.current.clientHeight;

                // Keep logo within bounds after resize
                let newX = positionRef.current.x;
                let newY = positionRef.current.y;

                if (newX + logoWidth > containerWidth) {
                    newX = containerWidth - logoWidth;
                }

                if (newY + logoHeight > containerHeight) {
                    newY = containerHeight - logoHeight;
                }

                positionRef.current = { x: newX, y: newY };
                logoRef.current.style.left = `${newX}px`;
                logoRef.current.style.top = `${newY}px`;
            }
        };

        window.addEventListener('resize', handleResize);
        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, []);

    return (
        <div className="dvd-container" ref={containerRef}>
            <div
                className="dvd-logo"
                ref={logoRef}
                style={{
                    color: color,
                    borderColor: color
                }}
            >
                <div className="logo-content">
                    <div className="logo-text">alto</div>
                    <div className="maintenance-text">
                        <p>Under Maintenance</p>
                        <p className="small-text">Follow <a href="https://x.com/commonwarexyz" target="_blank" rel="noopener noreferrer">@commonwarexyz</a> for updates</p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MaintenancePage;