import React, { useEffect } from 'react';

interface AboutModalProps {
    isOpen: boolean;
    onClose: () => void;
}

const AboutModal: React.FC<AboutModalProps> = ({ isOpen, onClose }) => {
    // Add effect to handle link targets
    useEffect(() => {
        if (isOpen) {
            // Find all links in the modal and set them to open in new tabs
            const modalLinks = document.querySelectorAll('.about-modal-content a');
            modalLinks.forEach(link => {
                if (link instanceof HTMLAnchorElement) {
                    link.setAttribute('target', '_blank');
                    link.setAttribute('rel', 'noopener noreferrer');
                }
            });
        }
    }, [isOpen]);
    if (!isOpen) return null;

    return (
        <div className="about-modal-overlay">
            <div className="about-modal">
                <div className="about-modal-header">
                    <h2>alto: <i>A minimal blockchain built with the Commonware Library</i></h2>
                </div>
                <div className="about-modal-content">
                    <section>
                        <h3>About</h3>
                        <p>
                            This explorer visualizes <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/threshold_simplex/index.html">alto's consensus</a> in real time.
                        </p>
                        <p>
                            Using the <i>network key</i> displayed at the top of the page, your browser verifies that any data
                            displayed was signed by at least <i>2f+1</i> of the <i>3f+1</i> validators in the network using WASM-compiled
                            cryptography from the Commonware Library.
                        </p>
                    </section>

                    <section>
                        <h3>Who Hosts this Example?</h3>
                        <p>
                            All data you see is relayed from consensus to your browser with <a href="https://exoware.xyz">exoware::relay</a>.
                        </p>
                        <p>
                            If you want to replay any of the stream, checkout <a href="https://TODO">inspector</a>.


                            <pre className="code-block">
                                <code>
                                    cargo install alto-inspector
                                </code>
                            </pre>

                            <pre className="code-block">
                                <code>
                                    inspector get block 10
                                </code>
                            </pre>
                        </p>
                    </section>

                    <section>
                        <h3>The Map</h3>
                        <p>
                            The map displays the location of the current leader node that is proposing the block for the most recent view.
                            Leader selection is determined by a Verifiable Random Function (VRF) using the node's signature.
                        </p>
                        <p>
                            This geographic distribution helps visualize how the consensus process works across a globally distributed network.
                        </p>
                    </section>

                    <section>
                        <h3>Understanding the Visualization</h3>
                        <p>
                            The dashboard shows a series of <strong>views</strong> - discrete consensus rounds where the network attempts
                            to agree on a block. Each view progresses through several stages:
                        </p>
                        <ul className="status-list">
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#0000eeff" }}></div>
                                    <strong>VRF</strong>
                                </div>
                                The view is in progress. The leader (selected via VRF)
                                is proposing a block to be agreed upon.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#000" }}></div>
                                    <strong>Locked</strong>
                                </div>
                                The view has received enough validator signatures
                                to be considered notarized. This means a quorum of validators has endorsed this block.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#274e13ff" }}></div>
                                    <strong>Finalized</strong>
                                </div>
                                The view has been fully confirmed by the network and
                                the block is now immutable.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#f4ccccff" }}></div>
                                    <strong>Timed Out</strong>
                                </div>
                                The view failed to progress within the expected timeframe.
                            </li>
                        </ul>
                        <p>
                            The latency values (in milliseconds) shown below each bar indicate how long each phase took to complete.
                        </p>
                    </section>

                </div>
                <div className="about-modal-footer">
                    <button className="about-button" onClick={onClose}>Close</button>
                </div>
            </div>
        </div >
    );
};

export default AboutModal;