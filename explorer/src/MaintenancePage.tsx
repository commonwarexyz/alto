import React, { useState, useEffect, useLayoutEffect, useRef } from 'react';
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

    // Cache the logo's rendered size so the animation loop does not have to re-measure it.
    const measureLogo = () => {
        if (!logoRef.current) {
            return;
        }
        const rect = logoRef.current.getBoundingClientRect();
        logoDimensionsRef.current = { width: rect.width, height: rect.height };
    };

    // Place the logo at a random starting position before the first paint. A layout effect runs
    // after layout but before the browser draws, so the logo is never shown anywhere else first.
    useLayoutEffect(() => {
        if (initializedRef.current || !containerRef.current || !logoRef.current) {
            return;
        }
        measureLogo();
        const containerWidth = containerRef.current.clientWidth;
        const containerHeight = containerRef.current.clientHeight;
        const { width: logoWidth, height: logoHeight } = logoDimensionsRef.current;
        positionRef.current = {
            x: Math.random() * Math.max(0, containerWidth - logoWidth),
            y: Math.random() * Math.max(0, containerHeight - logoHeight),
        };
        logoRef.current.style.left = `${positionRef.current.x}px`;
        logoRef.current.style.top = `${positionRef.current.y}px`;
        initializedRef.current = true;
    }, []);

    // The logo's size can change once web fonts finish loading; refresh the cached size then.
    useEffect(() => {
        let cancelled = false;
        document.fonts?.ready.then(() => {
            if (!cancelled) {
                measureLogo();
            }
        });
        return () => {
            cancelled = true;
        };
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
            if (!initializedRef.current || !containerRef.current || !logoRef.current) {
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
                        <p>SYSTEM MAINTENANCE</p>
                        <p className="small-text">Follow <span className="link-text">@commonwarexyz</span> for updates and new releases.</p>
                    </div>
                </div>
            </div>
        </div >
    );
};

export default MaintenancePage;