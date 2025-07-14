import React, { useEffect } from 'react';
import { Cluster, ClusterConfig } from './config';

interface AboutModalProps {
    isOpen: boolean;
    onClose: () => void;
    selectedCluster: Cluster;
    onClusterChange: (cluster: Cluster) => void;
    configs: Record<Cluster, ClusterConfig>;
}

const AboutModal: React.FC<AboutModalProps> = ({ isOpen, onClose, selectedCluster, onClusterChange, configs }) => {
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
                        <h3>Select Cluster</h3>
                        <div className="cluster-selection">
                            <div className="cluster-options">
                                {Object.entries(configs).map(([clusterId, config]) => (
                                    <button
                                        key={clusterId}
                                        className={`cluster-option ${selectedCluster === clusterId ? 'selected' : ''}`}
                                        onClick={() => onClusterChange(clusterId as Cluster)}
                                    >
                                        <div className="cluster-option-name">{config.name}</div>
                                        <div className="cluster-option-summary">{config.name === 'Global Cluster' ? 'Multi-region deployment' : 'US-only deployment'}</div>
                                    </button>
                                ))}
                            </div>
                            <div className="cluster-description" dangerouslySetInnerHTML={{ __html: configs[selectedCluster].description }} />
                        </div>
                    </section>

                    <section>
                        <h3>What is alto?</h3>
                        <p>
                            <a href="https://github.com/commonwarexyz/alto">alto</a> is a minimal (and wicked fast) blockchain built with the <a href="https://github.com/commonwarexyz/monorepo">Commonware Library</a>.
                        </p>
                        <p>
                            By minimal, we mean minimal. alto's state transition function consists of just <strong>3 rules</strong>. Each block must:
                        </p>
                        <ul>
                            <li>Increase the height by 1</li>
                            <li>Reference the digest of its parent</li>
                            <li>Propose a new timestamp greater than its parent (<i>but not more than 500ms in the future</i>)</li>
                        </ul>
                        <p>
                            That's it!
                        </p>
                    </section>

                    <section>
                        <h3>What are you looking at?</h3>
                        <p>
                            This explorer displays the progression of <i>threshold-simplex</i> over time, broken into <strong>views</strong>.
                        </p>
                        <p>
                            Validators enter a new view whenever they observe either <i>2f+1</i> votes for a block proposal or <i>2f+1</i> nullifies
                            (to skip this view) AND some seed (a VRF used to select the next leader). Validators finalize a view whenever they
                            observe <i>2f+1</i> finalizes for a block proposal.
                        </p>
                        <p>
                            We color the phases of a view as follows:
                        </p>
                        <ul className="status-list">
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#0000eeff" }}></div>
                                    <strong>Seeded</strong>
                                </div>
                                Some leader has been elected to propose a block. The dot on the map (of the same color) is the region where the leader is located.
                                A new leader is elected for each view.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#000" }}></div>
                                    <strong>Locked</strong>
                                </div>
                                Some block <i>b</i> has received <i>2f+1</i> votes in a given view <i>v</i>. This means there can never be another locked block
                                in view <i>v</i> (and block <i>b</i> must be used in the canonical chain if <i>2f+1</i> participants did not move to nullify).
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#228B22ff" }}></div>
                                    <strong>Finalized</strong>
                                </div>
                                The block <i>b</i> in view <i>v</i> has received <i>2f+1</i> finalizes. The block is now immutable.
                            </li>
                        </ul>
                        <p>
                            You can read more about the design of <i>threshold-simplex</i> <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/threshold_simplex/index.html">here</a>.
                        </p>
                    </section>
                    <section>
                        <h3>Why is it so fast?</h3>
                        <p>
                            <i>threshold-simplex</i>, like <a href="https://eprint.iacr.org/2023/463">Simplex Consensus</a>, employs <strong>all-to-all broadcast</strong> and <strong>progress-driven view transitions</strong> to
                            achieve optimal latency in both the optimistic and pessimistic case (under the partial synchrony model).
                        </p>
                        <p>
                            Using authenticated connections (provided by <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a>), each validator
                            sends consensus messages directly to every other validator (no leader relay or multi-hop gossip). As soon as any validator observes <i>2f+1</i> votes for a block proposal, they broadcast
                            a threshold signature (again directly) to all other validators and enter the next view immediately (without waiting for finalization or any timeout). If a validator sees a threshold signature
                            for a view <i>v</i>, they enter view <i>v+1</i> immediately (ensuring validators stay synchronized without using a clock).
                        </p>
                        <p>
                            English? <i>threshold-simplex</i> moves at <strong>network speed</strong> (and it turns out that's pretty fast in 2025).
                        </p>
                    </section>
                    <section>
                        <h3>Can I replay the stream?</h3>
                        <p>
                            Yes! You can replay the stream or fetch arbitrary data using the <a href="https://docs.rs/alto-inspector/latest/alto_inspector">alto-inspector</a>.
                        </p>
                        <p>
                            To download the tool, run:
                            <pre className="code-block">
                                <code>
                                    cargo install alto-inspector
                                </code>
                            </pre>

                            And then, to fetch block 10, run:
                            <pre className="code-block">
                                <code>
                                    inspector get block 10
                                </code>
                            </pre>
                        </p>
                    </section>
                    <section>
                        <h3>Support</h3>
                        <p>If you run into any issues or have any other questions, <a href="https://github.com/commonwarexyz/alto/issues">open an issue!</a></p>
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