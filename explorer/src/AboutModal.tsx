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
                            <a href="https://github.com/commonwarexyz/alto">alto</a> is a minimal (and wicked fast) blockchain built with the <a href="https://github.com/commonwarexyz/monorepo">Commonware Library</a>.
                        </p>
                        <p>
                            By minimal, we mean minimal. alto's state transition function consists of just <strong>3 rules</strong>. Each block must:
                            <ul>
                                <li>Increase the height by 1</li>
                                <li>Reference the digest of its parent</li>
                                <li>Propose a new timestamp greater than its parent (<i>but not more than 500ms in the future</i>)</li>
                            </ul>
                        </p>
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
                                    <strong>Seed</strong>
                                </div>
                                Some leader has been elected to propose a block. The dot on the map (of the same color) is the region where the leader is located.
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
                                    <div className="about-status-indicator" style={{ backgroundColor: "#274e13ff" }}></div>
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
                            operate at optimal latency (under the partial synchrony model).
                        </p>
                        <p>
                            English? All validators in alto are connected directly to each other (using <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a>) and send
                            consensus messages directly to each other (without going through a "leader relay"). As soon as any validator observes <i>2f+1</i> votes for a block proposal, the broadcast a threshold signature
                            representing their observation (by combining the <i>2f+1</i> partial signatures observed) and enter the next view immediately (before waiting for finalization or any timeout). While working on the next view,
                            validators continue working towards finalization of the previous view (in parallel).
                        </p>
                        <p>

                        </p>
                    </section>
                    <section>
                        <h3>Where is the data coming from?</h3>
                        <p>
                            To power this explorer, we deployed alto to a cluster of <strong>25 c7g.xlarge</strong> nodes (4 vCPU, 8GB RAM) on AWS in <strong>10 regions</strong> (us-west-1, us-east-1, eu-west-1, ap-northeast-1, eu-north-1, ap-south-1, sa-east-1, eu-central-1, ap-northeast-2, ap-southeast-2)
                            and built some infrastructure to stream each consensus message to your browser in real time (<a href="https://exoware.xyz">exoware::relay</a>).
                        </p>
                        <p>
                            Because each consensus artifact is accompanied by a threshold signature (the public key of which is the <strong>network key</strong> displayed at the top of the page), your browser can (and does) verify each inbound message using <a href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/index.html">cryptography::bls12381</a> compiled to WASM.
                        </p>
                        <p>
                            That's right, your browser is verifying every message you receive was produced form some known consensus set in real time. Don't trust our infrastructure, trust the open source verifier code running on your computer.
                        </p>
                    </section>
                    <section>
                        <h3>How do I measure latency?</h3>
                        <p>
                            Your browser measures latency by comparing the timestamp of a block (referenced in a prepared or finalization artifact) to your local time. This means that the latency you see includes the time it takes for a block to be proposed, voted upon, sent to
                            exoware::relay, and then received by your browser.
                        </p>
                        <p>
                            While it is true that validators observe lower latency, the one that usually impacts UX is the one you're measuring here.
                        </p>
                    </section>
                    <section>
                        <h3>Can I replay the stream?</h3>
                        <p>
                            Yes! You can replay the stream or fetch arbitrary data using the <a href="https://docs.rs/alto-inspector/latest/alto_inspector/">alto-inspector</a>. To download the tool, run:
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