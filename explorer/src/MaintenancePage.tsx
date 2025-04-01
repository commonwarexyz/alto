import React, { useState, useEffect, useRef } from 'react';
import './MaintenancePage.css';

const MaintenancePage: React.FC = () => {
    const containerRef = useRef<HTMLDivElement>(null);
    const logoRef = useRef<HTMLDivElement>(null);
    const positionRef = useRef({ x: 50, y: 50 });
    const directionRef = useRef({ x: 1, y: 1 });
    const [color, setColor] = useState('#0000ee');
    const currentColorRef = useRef('#0000ee');
    const animationFrameRef = useRef<number | null>(null);
    const initializedRef = useRef(false);

    // Speed in pixels per frame
    const speed = 0.5;

    // Use a ref to store the logo's natural dimensions
    const logoDimensionsRef = useRef({ width: 0, height: 0 });

    // Handle click on the logo
    const handleLogoClick = () => {
        window.open("https://x.com/commonwarexyz", "_blank", "noopener,noreferrer");
    };

    // Measure logo once after initial render
    useEffect(() => {
        const measureLogo = () => {
            if (logoRef.current && containerRef.current) {
                // Get the natural dimensions of the logo
                const rect = logoRef.current.getBoundingClientRect();
                logoDimensionsRef.current = {
                    width: rect.width,
                    height: rect.height
                };

                console.log("Measured logo: ", logoDimensionsRef.current);
            }
        };

        // Measure immediately and after a short delay to ensure accuracy
        measureLogo();
        const timer = setTimeout(measureLogo, 200);

        return () => clearTimeout(timer);
    }, []);

    useEffect(() => {
        // Wait a short time to ensure logo has been properly measured
        const initTimeout = setTimeout(() => {
            if (!initializedRef.current && containerRef.current && logoRef.current) {
                const containerWidth = containerRef.current.clientWidth;
                const containerHeight = containerRef.current.clientHeight;

                // Make sure we have measured the logo
                if (logoDimensionsRef.current.width === 0) {
                    const rect = logoRef.current.getBoundingClientRect();
                    logoDimensionsRef.current = {
                        width: rect.width,
                        height: rect.height
                    };
                }

                const logoWidth = logoDimensionsRef.current.width;
                const logoHeight = logoDimensionsRef.current.height;

                console.log("Container size: ", containerWidth, containerHeight);
                console.log("Logo size: ", logoWidth, logoHeight);

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
        }, 100); // Short delay to ensure measurements

        return () => clearTimeout(initTimeout);
    }, []);

    useEffect(() => {
        // Move colors array outside the component to avoid dependency issues
        const colors = [
            '#0000ee', '#ee0000', '#00ee00', '#ee00ee',
            '#eeee00', '#00eeee', '#ff7700', '#7700ff'
        ];

        // Get a random color that's different from the current one
        const getRandomColor = () => {
            const filteredColors = colors.filter(c => c !== currentColorRef.current);
            return filteredColors[Math.floor(Math.random() * filteredColors.length)];
        };

        // Update color function that ensures the color always changes
        const updateColor = () => {
            const newColor = getRandomColor();
            currentColorRef.current = newColor;
            setColor(newColor);
        };

        // Animation function that doesn't depend on React state for positioning
        const animate = () => {
            if (!containerRef.current || !logoRef.current) {
                animationFrameRef.current = requestAnimationFrame(animate);
                return;
            }

            const containerWidth = containerRef.current.clientWidth;
            const containerHeight = containerRef.current.clientHeight;

            // Use our stored dimensions to avoid recalculating during animation
            const logoWidth = logoDimensionsRef.current.width;
            const logoHeight = logoDimensionsRef.current.height;

            // Update position based on current direction
            let newX = positionRef.current.x + speed * directionRef.current.x;
            let newY = positionRef.current.y + speed * directionRef.current.y;
            let colorChanged = false;

            // Handle horizontal boundaries with a small buffer
            const rightEdgeThreshold = containerWidth - logoWidth;
            if (newX <= 0) {
                // Hit left edge
                directionRef.current.x = Math.abs(directionRef.current.x); // Ensure positive
                newX = 0; // Stop at boundary
                if (!colorChanged) {
                    updateColor();
                    colorChanged = true;
                }
            } else if (newX >= rightEdgeThreshold) {
                // Hit right edge
                directionRef.current.x = -Math.abs(directionRef.current.x); // Ensure negative
                newX = rightEdgeThreshold; // Stop exactly at boundary
                if (!colorChanged) {
                    updateColor();
                    colorChanged = true;
                }
            }

            // Handle vertical boundaries with a small buffer
            const bottomEdgeThreshold = containerHeight - logoHeight;
            if (newY <= 0) {
                // Hit top edge
                directionRef.current.y = Math.abs(directionRef.current.y); // Ensure positive
                newY = 0; // Stop at boundary
                if (!colorChanged) {
                    updateColor();
                    colorChanged = true;
                }
            } else if (newY >= bottomEdgeThreshold) {
                // Hit bottom edge
                directionRef.current.y = -Math.abs(directionRef.current.y); // Ensure negative
                newY = bottomEdgeThreshold; // Stop exactly at boundary
                if (!colorChanged) {
                    updateColor();
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
    }, []);

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
                onClick={handleLogoClick}
            >
                <div className="logo-content">
                    <div className="maintenance-text">
                        <p>UPGRADE DEPLOYING</p>
                        <p className="small-text">Monitor progress<br />at <span className="link-text">@commonwarexyz</span>.</p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MaintenancePage;