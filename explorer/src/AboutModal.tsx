import React, { useEffect } from 'react';
import { ClusterConfig, getHttpBackendUrl, MODE } from './config';
import { getLeaderIndicator, getTimelineIdentifier } from './timelineIdentifier';

interface AboutModalProps {
    isOpen: boolean;
    onClose: () => void;
    clusterConfig: ClusterConfig;
}

const AboutModal: React.FC<AboutModalProps> = ({ isOpen, onClose, clusterConfig }) => {
    const standardCertificates = clusterConfig.CERTIFICATE_MODE === 'standard';
    const timelineIdentifier = getTimelineIdentifier(standardCertificates);
    const leaderIndicator = getLeaderIndicator(standardCertificates);
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

    const [certificateMode, indexer, identity] = [
        clusterConfig.CERTIFICATE_MODE,
        getHttpBackendUrl(clusterConfig.BACKEND_URL),
        clusterConfig.PUBLIC_KEY_HEX,
    ].map(value => `'${value.replace(/'/g, "'\\''")}'`);

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
                            This explorer visualizes the performance of <a href="https://github.com/commonwarexyz/alto">alto</a>'s consensus, <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/simplex/index.html">simplex</a>, running on the selected cluster.
                        </p>
                        {MODE === 'public' && (
                            <p>
                                <i>You can replicate this devnet in your own AWS account with <a href="https://docs.rs/commonware-deployer/latest/commonware_deployer/aws/">commonware_deployer::aws</a> by following the
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
                            alto's state transition function has <strong>4 rules</strong>. Each block after genesis must:
                        </p>
                        <ul>
                            <li>Increase the height by 1</li>
                            <li>Reference the digest of its parent</li>
                            <li>Increase its parent's timestamp without exceeding January 1, 2200 (UTC)</li>
                            <li>Contain exactly the configured number of opaque payload bytes (zero by default)</li>
                        </ul>
                        <p>
                            Validators wait until a valid block's timestamp is no more than one second ahead of their local clock before certifying it.
                        </p>
                    </section>

                    <section>
                        <h3>What are you looking at?</h3>
                        <p>
                            This explorer displays the progression of <i>simplex</i> over time, broken into <strong>views</strong>.
                        </p>
                        <p>
                            In stable mode, a <strong>term</strong> groups consecutive views under one round-robin leader. In rotating mode,
                            each term contains one view and a VRF seed selects its leader.
                        </p>
                        <p>
                            A quorum of <i>2f+1</i> votes for a block forms a <strong>notarization</strong>. After successfully certifying the block,
                            validators advance to the next view without waiting for finalization. A quorum of <i>2f+1</i> nullifies forms a <strong>nullification</strong>,
                            which abandons the remaining term and advances to the first view of the next term.
                            Validators finalize a block when they observe <i>2f+1</i> finalizes for it.
                        </p>
                        <p>
                            We color the phases of a view as follows:
                        </p>
                        <ul className="status-list">
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: leaderIndicator.color }}></div>
                                    <strong>{leaderIndicator.label}</strong>
                                </div>
                                {standardCertificates
                                    ? 'The stable round-robin leader has proposed a block.'
                                    : 'Some leader has been elected to propose a block.'}
                                {MODE === 'public' && " A map dot of the same color shows the leader's region when its location is known."}
                                {!standardCertificates && ' A new leader is elected for each view.'}
                            </li>
                            <li>
                                <div className="status-indicator-wrapper">
                                    <div className="about-status-indicator" style={{ backgroundColor: "#000" }}></div>
                                    <strong>Locked</strong>
                                </div>
                                Block <i>b</i> has received <i>2f+1</i> votes in view <i>v</i>, so no other block can be notarized in that view.
                                It must appear in the canonical chain unless a nullification covers <i>v</i>, from <i>v</i> itself or an earlier view in the same term.
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
                            The short <span style={{ color: timelineIdentifier.color }}>{timelineIdentifier.label.toLowerCase()}</span> beneath each view number identifies {standardCertificates ? 'the block proposed in that view' : 'the seed used for leader election'}.
                        </p>
                        <p>
                            You can read more about the design of <i>simplex</i> <a href="https://docs.rs/commonware-consensus/latest/commonware_consensus/simplex/index.html">here</a>.
                        </p>
                    </section>

                    <section>
                        <h3>Where is the data coming from?</h3>
                        <p dangerouslySetInnerHTML={{ __html: clusterConfig.description }} />
                        <p>
                            Consensus artifacts stream to your browser in real time. An <a href="https://github.com/commonwarexyz/alto/tree/main/indexer">alto-indexer</a> can serve both this explorer and its consensus stream from the same address.
                        </p>
                        <p>
                            Before using a consensus artifact, your browser verifies its threshold signatures with <a href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/index.html">cryptography::bls12381</a> compiled to WASM.
                        </p>
                        <p>
                            Older artifacts may be skipped when verification falls behind the stream. The open source verifier runs on your computer, so the API's consensus claims are checked locally.
                        </p>
                    </section>

                    <section>
                        <h3>Why is it so fast?</h3>
                        <p>
                            <i>simplex</i>, like <a href="https://eprint.iacr.org/2023/463">Simplex Consensus</a>, employs <strong>all-to-all broadcast</strong> and <strong>progress-driven view transitions</strong> to
                            achieve optimal latency in both the optimistic and pessimistic case (under the partial synchrony model).
                        </p>
                        <p>
                            Using authenticated connections (provided by <a href="https://docs.rs/commonware-p2p/latest/commonware_p2p/authenticated/index.html">p2p::authenticated</a>), each validator
                            sends consensus messages directly to every other validator (no leader relay or multi-hop gossip). A validator that collects <i>2f+1</i> votes broadcasts the resulting notarization.
                            Validators advance from view <i>v</i> to <i>v+1</i> once they successfully certify the block, without waiting for finalization or a timeout.
                            This lets successful views progress without a scheduled round boundary.
                        </p>
                        <p>
                            English? <i>simplex</i> moves at <strong>network speed</strong> (and it turns out that's pretty fast in 2025).
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
                                {`inspector --certificate-mode ${certificateMode} get block 10 --indexer ${indexer} --identity ${identity}`}
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
