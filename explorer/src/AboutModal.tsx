import React, { useState } from 'react';

interface AboutModalProps {
    isOpen: boolean;
    onClose: () => void;
}

const AboutModal: React.FC<AboutModalProps> = ({ isOpen, onClose }) => {
    if (!isOpen) return null;

    return (
        <div className="about-modal-overlay">
            <div className="about-modal">
                <div className="about-modal-header">
                    <h2>alto: <i>The first blockchain built with the Commonware Library.</i></h2>
                </div>
                <div className="about-modal-content">
                    <section>
                        <h3>About</h3>
                        <p>
                            This dashboard visualizes the alto consensus protocol in real-time. alto is a Byzantine Fault Tolerant (BFT)
                            consensus mechanism that ensures distributed systems can reach agreement even when some participants may be faulty
                            or malicious.
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
                        <h3>Network Key</h3>
                        <p>
                            The Network Key displayed at the top of the dashboard is the public key used to verify signatures from
                            the network. This ensures that all messages received are authentic and come from authorized participants.
                        </p>
                    </section>

                    <section>
                        <h3>Technical Details</h3>
                        <p>
                            This visualization connects to the alto consensus network via WebSocket and processes three main types of messages:
                        </p>
                        <ul>
                            <li><strong>Seed:</strong> Indicates the start of a new view with leader selection</li>
                            <li><strong>Notarization:</strong> Represents quorum certificate formation</li>
                            <li><strong>Finalization:</strong> Signals final commitment of a block</li>
                        </ul>
                        <p>
                            Each message is cryptographically verified using the network public key to ensure authenticity.
                        </p>
                    </section>
                </div>
                <div className="about-modal-footer">
                    <button className="about-button" onClick={onClose}>Close</button>
                </div>
            </div>
        </div>
    );
};

export default AboutModal;