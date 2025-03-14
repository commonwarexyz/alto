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
            const modalLinks = document.querySelectorAll('.about-modal a');
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
                    <h2>Welcome to the <a href="https://github.com/commonwarexyz/alto">alto</a> Explorer!</h2>
                </div>
                <div className="about-modal-content">
                    <section>
                        <h3>About</h3>
                        <p>
                            This explorer visualizes the performance of <a href="https://github.com/commonwarexyz/alto">alto</a>'s consensus, <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/threshold_simplex/index.html">threshold-simplex</a>,
                            deployed on a cluster of globally distributed nodes.
                        </p>
                        <p>
                            <i>You can replicate this devnet in your own AWS account with <a href="https://docs.rs/commonware-deployer/0.0.41/commonware_deployer/">deployer::ec2</a> by following the
                                instructions <a href="https://github.com/commonwarexyz/alto/blob/main/chain/README.md">here</a>.</i>
                        </p>
                    </section>

                    <section>
                        <h3>What is alto?</h3>
                        <p>
                            <a href="https://github.com/commonwarexyz/alto">alto</a> is a minimal (and wicked fast) blockchain built with the Commonware Library.
                        </p>
                        <p>
                            By minimal, we mean minimal. alto's state transition function consists of just <strong>3</strong> rules. Each block must:
                            <ul>
                                <li>Increase the height by 1</li>
                                <li>Reference the digest of its parent</li>
                                <li>Propose a new timestamp greater than its parent (<i>but not more than 500ms in the future</i>)</li>
                            </ul>
                        </p>
                        <p>
                            TODO: HOOK
                        </p>
                    </section>

                    <section>
                        <h3>What are you looking at?</h3>
                        <p>
                            The dashboards on this explorer display the progression of <i>threshold-simplex</i> over time, broken into <strong>views</strong>. Participants in consensus enter a new
                            view <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/threshold_simplex/index.html#specification-for-view-v">whenever they observe either <i>2f+1</i> votes for a block proposal or a timeout (<i>they may see both</i>)</a>.
                        </p>
                        <p>
                            threshold-simplex, like <a href="https://eprint.iacr.org/2023/463">Simplex Consensus</a>, is optimistically responsive and tolerates up to <i>f</i> Byzantine faults in the partially synchronous setting. English? When the leader is honest and the network is healthy,
                            participants come to agreement at <strong>network speed</strong>.
                        </p>
                        <p>
                            When every participant is directly connected to every other participant (alto uses <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a>) and leader's don't "relay" aggregated/recovered signatures, it turns out network
                            speed is <strong>really fast</strong> (even when participants are spread across the globe).
                        </p>
                        <p>
                            Unlike other popular consensus constructions, threshold-simplex does not employ a "leader relay" to minimize view latency and instead requires each participant to broadcast each message to all other parties (i.e. all-to-all). In alto, we use <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a> to form an encrypted connection to each participant in consensus.
                        </p>
                        <p>
                            threshold-simplex, unlike Simplex Consensus, introduces <i>BLS12-381 Threshold Signatures</i> for all messages.

                            threshold-simplex is optimstically responsive

                            On the "happy path", participants move at <strong>network speed</strong> between views (only ever relying on a timeout for a <a href="https://github.com/commonwarexyz/monorepo/blob/04814db4ce7db6d2753ac33878a827e7176d6dfc/consensus/src/threshold_simplex/mod.rs#L79">recently inactive leader</a>).


                            This site visualizes the progression of <i>threshold-simplex</i> over time, represented by <strong>views</strong>.


                            across <strong>views</strong> -.


                            of block proposal, notarization, and finalization on the alto devnet

                            shows a series of <strong>views</strong> - discrete consensus rounds where the network attempts
                            to agree on a block. Each view progresses through several stages:
                        </p>
                        <ul className="status-list">
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#0000eeff" }}></div>
                                    <strong>Seed</strong>
                                </div>
                                The view is in progress. The leader (selected via VRF)
                                is proposing a block to be agreed upon.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#000" }}></div>
                                    <strong>Prepared</strong>
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
                        <h3>Who Hosts this Example?</h3>
                        <p>
                            Using the <i>network key</i> displayed at the top of the page, your browser verifies that any data
                            displayed was signed by at least <i>2f+1</i> of the <i>3f+1</i> validators in the network using WASM-compiled
                            cryptography from the Commonware Library.
                        </p>
                        <p>
                            All data you see is relayed from consensus to your browser with <a href="https://exoware.xyz">exoware::relay</a>.
                        </p>
                        <p>
                            If you want to replay any of the stream, checkout <a href="https://docs.rs/crate/alto-inspector/latest">inspector</a>.


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
                        <h3>Configuration</h3>
                        <p>
                            This geographic distribution helps visualize how the consensus process works across a globally distributed network.

                            instance size
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