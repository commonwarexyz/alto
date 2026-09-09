import React, { useEffect, useRef } from 'react';
import './MaintenancePage.css';

const MaintenancePage: React.FC = () => {
    const containerRef = useRef<HTMLDivElement>(null);
    const logoRef = useRef<HTMLDivElement>(null);
    const positionRef = useRef({ x: 0, y: 0 });
    const directionRef = useRef({ x: 1, y: 1 });
    const currentColorRef = useRef('#0000ee');
    const animationFrameRef = useRef<number | null>(null);
    const lastFrameRef = useRef<number | null>(null);
    const initializedRef = useRef(false);

    // Speed in CSS pixels per second (independent of the display's refresh rate)
    const speed = 30;

    // Use a ref to store the logo's natural dimensions
    const logoDimensionsRef = useRef({ width: 0, height: 0 });

    // Handle click on the logo
    const handleLogoClick = () => {
        window.open("https://x.com/commonwarexyz", "_blank", "noopener,noreferrer");
    };

    // Move the logo with a transform to avoid layout work during animation.
    const applyPosition = () => {
        if (logoRef.current) {
            const { x, y } = positionRef.current;
            logoRef.current.style.transform = `translate3d(${x}px, ${y}px, 0)`;
        }
    };

    // Cache the logo's rendered size so the animation loop does not have to re-measure it.
    const measureLogo = () => {
        if (!logoRef.current) {
            return;
        }
        const rect = logoRef.current.getBoundingClientRect();
        logoDimensionsRef.current = { width: rect.width, height: rect.height };
    };

    // Reveal after font loading or 1.5 seconds. Re-measure on later font completion and resize
    // to keep the logo within the container as its font and responsive size change.
    useEffect(() => {
        // Refresh the cached box size and keep the box inside the container.
        const keepInBounds = () => {
            if (!containerRef.current || !logoRef.current) {
                return;
            }
            measureLogo();
            const maxX = Math.max(0, containerRef.current.clientWidth - logoDimensionsRef.current.width);
            const maxY = Math.max(0, containerRef.current.clientHeight - logoDimensionsRef.current.height);
            positionRef.current = {
                x: Math.min(positionRef.current.x, maxX),
                y: Math.min(positionRef.current.y, maxY),
            };
            applyPosition();
        };
        const reveal = () => {
            if (!containerRef.current || !logoRef.current) {
                return;
            }
            if (initializedRef.current) {
                keepInBounds();
                return;
            }
            measureLogo();
            const { width, height } = logoDimensionsRef.current;
            positionRef.current = {
                x: Math.random() * Math.max(0, containerRef.current.clientWidth - width),
                y: Math.random() * Math.max(0, containerRef.current.clientHeight - height),
            };
            applyPosition();
            logoRef.current.style.visibility = 'visible';
            initializedRef.current = true;
        };

        // A rejected load (blocked or failed download) reveals the box in the fallback font.
        const fallback = setTimeout(reveal, 1500);
        const font: Promise<unknown> = document.fonts
            ? document.fonts.load('bold 32px Inconsolata')
            : Promise.resolve();
        font.then(reveal, reveal);
        window.addEventListener('resize', keepInBounds);
        return () => {
            clearTimeout(fallback);
            window.removeEventListener('resize', keepInBounds);
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

        // Apply bounce colors directly to keep animation independent of React renders.
        const updateColor = () => {
            const newColor = getRandomColor();
            currentColorRef.current = newColor;
            if (logoRef.current) {
                logoRef.current.style.color = newColor;
            }
        };

        // Animation function that doesn't depend on React state for positioning
        const animate = (timestamp: number) => {
            if (!initializedRef.current || !containerRef.current || !logoRef.current) {
                animationFrameRef.current = requestAnimationFrame(animate);
                return;
            }

            // Use elapsed time for smooth motion and cap the step to prevent jumps when a
            // background tab becomes visible again.
            const elapsed = lastFrameRef.current === null ? 0 : timestamp - lastFrameRef.current;
            lastFrameRef.current = timestamp;
            const step = speed * Math.min(elapsed, 100) / 1000;

            const containerWidth = containerRef.current.clientWidth;
            const containerHeight = containerRef.current.clientHeight;

            // Use our stored dimensions to avoid recalculating during animation
            const logoWidth = logoDimensionsRef.current.width;
            const logoHeight = logoDimensionsRef.current.height;

            // Update position based on current direction
            let newX = positionRef.current.x + step * directionRef.current.x;
            let newY = positionRef.current.y + step * directionRef.current.y;
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

            positionRef.current = { x: newX, y: newY };
            applyPosition();

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

    return (
        <div className="dvd-container" ref={containerRef}>
            <div
                className="dvd-logo"
                ref={logoRef}
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
