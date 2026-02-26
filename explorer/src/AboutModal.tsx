import React, { useEffect } from 'react';
import { MODE } from './config';

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
                            This explorer visualizes the performance of <a href="https://github.com/commonwarexyz/alto">alto</a>'s consensus, <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/minimmit/index.html">minimmit</a>,
                            {MODE === 'public' ? ' deployed on a cluster of globally distributed nodes.' : ' running on your local machine.'}
                        </p>
                        {MODE === 'public' && (
                            <p>
                                <i>You can replicate this devnet in your own AWS account with <a href="https://docs.rs/commonware-deployer/latest/commonware_deployer/">deployer::ec2</a> by following the
                                    instructions <a href="https://github.com/commonwarexyz/alto/blob/main/deploy/README.md">here</a>.</i>
                            </p>
                        )}
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
                            This explorer displays the progression of <i>minimmit</i> over time, broken into <strong>views</strong>.
                        </p>
                        <p>
                            Minimmit uses round-robin leader election. There are no seed certificates.
                            A view is m-notarized when validators observe <i>2f+1</i> notarize votes for a proposal,
                            and finalized when validators observe <i>n-f</i> notarize votes for the same proposal.
                        </p>
                        <p>
                            We color the phases of a view as follows:
                        </p>
                        <ul className="status-list">
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#0000eeff" }}></div>
                                    <strong>Proposed</strong>
                                </div>
                                The round-robin leader for this view has proposed a block.
                                {MODE === 'public' && ' The dot on the map (of the same color) is the region where the leader is located.'}
                                {' '}Leaders advance deterministically by view.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#000" }}></div>
                                    <strong>M-Notarized</strong>
                                </div>
                                Some block <i>b</i> has received <i>2f+1</i> notarize votes in view <i>v</i>.
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#228B22ff" }}></div>
                                    <strong>Finalized</strong>
                                </div>
                                The block <i>b</i> in view <i>v</i> has received <i>n-f</i> notarize votes and is now immutable.
                            </li>
                        </ul>
                        <p>
                            You can read more about the design of <i>minimmit</i> <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/minimmit/index.html">here</a>.
                        </p>
                    </section>

                    <section>
                        <h3>Where is the data coming from?</h3>
                        {MODE === 'public' ? (
                            <>
                                <p>
                                    We deployed alto to a cluster of <strong>50 validators</strong> running c8g.large (2 vCPU, 4GB RAM) nodes on AWS in two separate clusters:
                                </p>
                                <ul>
                                    <li><strong>Global Cluster</strong>: 10 regions (us-west-1, us-east-1, eu-west-1, ap-northeast-1, eu-north-1, ap-south-1, sa-east-1, eu-central-1, ap-northeast-2, ap-southeast-2).</li>
                                    <li><strong>USA Cluster</strong>: 4 regions (us-east-1, us-west-1, us-east-2, us-west-2).</li>
                                </ul>
                                <p>
                                    When you visit this page, however, you don't connect to any of those nodes. You connect to custom-built infrastructure (<a href="https://exoware.xyz">exoware::relay</a>) that streams consensus
                                    artifacts to your browser in real time.
                                </p>
                            </>
                        ) : (
                            <p>
                                The data is streamed from your local alto cluster via the <a href="https://github.com/commonwarexyz/alto/tree/main/indexer">alto-indexer</a>.
                            </p>
                        )}
                        <p>
                            Because each consensus artifact is accompanied by a threshold signature, your browser can (and does) verify each inbound message using <a href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/index.html">cryptography::bls12381</a> compiled to WASM.
                        </p>
                        <p>
                            That's right, your browser is verifying every message it receives was emitted from consensus in real time. Don't trust an API, trust the open source verifier code running on your computer.
                        </p>
                    </section>

                    <section>
                        <h3>Why is it so fast?</h3>
                        <p>
                            <i>minimmit</i> uses all-to-all authenticated broadcast and progress-driven view transitions to
                            achieve low latency under partial synchrony.
                        </p>
                        <p>
                            Using authenticated connections (provided by <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a>), each validator
                            sends consensus messages directly to every other validator (no leader relay or multi-hop gossip). As soon as any validator observes
                            enough votes for m-notarization or finalization, they broadcast threshold artifacts directly and advance views quickly.
                        </p>
                        <p>
                            English? <i>minimmit</i> moves at <strong>network speed</strong>.
                        </p>
                    </section>

                    <section>
                        <h3>Can I replay the stream?</h3>
                        <p>
                            Yes! You can replay the stream or fetch arbitrary data using the <a href="https://docs.rs/alto-inspector/latest/alto_inspector">alto-inspector</a>.
                        </p>
                        <p>
                            To download the tool, run:
                        </p>
                        <pre className="code-block">
                            <code>
                                cargo install alto-inspector
                            </code>
                        </pre>
                        <p>
                            And then, to fetch block 10, run:
                        </p>
                        <pre className="code-block">
                            <code>
                                inspector get block 10
                            </code>
                        </pre>
                    </section>

                    <section>
                        <h3>Support</h3>
                        <p>If you run into any issues or have any other questions, <a href="https://github.com/commonwarexyz/alto/issues">open an issue!</a></p>
                    </section>
                </div>
                <div className="about-modal-footer">
                    <button className="about-button" onClick={onClose}>Close</button>
                </div>
            </div >
        </div >
    );
};

export default AboutModal;
