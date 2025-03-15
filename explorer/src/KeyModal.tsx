import React, { useEffect } from 'react';
import './AboutModal.css'; // Sharing the same CSS file

interface KeyInfoModalProps {
    isOpen: boolean;
    onClose: () => void;
    publicKeyHex: string;
}

const KeyInfoModal: React.FC<KeyInfoModalProps> = ({ isOpen, onClose, publicKeyHex }) => {
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
                    <h2>Verifying Inbound Messages</h2>
                </div>
                <div className="about-modal-content">
                    <section>
                        <h3>I'm verifying threshold signatures?</h3>
                        <p>
                            When your browser receives a consensus message (seed, notarization, or finalization)
                            from the <a href="https://exoware.xyz">exoware::relay</a>:
                        </p>
                        <ol>
                            <li>The message arrives containing a <i>BLS12-381</i> signature.</li>
                            <li>Your browser uses <a href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/index.html">cryptography::bls12381</a> (compiled to WebAssembly) to verify this signature against the static <strong>Network Key</strong>.</li>
                            <li>If the signature is valid, the message is processed and displayed.</li>
                            <li>If invalid, the message is rejected.</li>
                        </ol>
                    </section>

                    <section>
                        <h3>What is a Network Key?</h3>
                        <p>
                            This key is a <i>BLS12-381</i> public key that represents a shared secret maintained by all validators. Any <strong>2f+1</strong> validators can use this shares of this key to sign messages on behalf of alto (e.g. during each consensus view).
                        </p>
                        <p>
                            The <strong>Network Key</strong> for alto is:
                            <pre className="code-block">
                                <code>{publicKeyHex}</code>
                            </pre>
                        </p>
                        <p>
                            <i>
                                In a production environment, you would hardcode this in your binary or store it locally rather than relying
                                on a website to provide it (like with <a href="https://docs.rs/alto-inspector/latest/alto_inspector">alto-inspector</a>).
                            </i>
                        </p>
                    </section>
                    <section>
                        <h3>Can validators be rotated if they maintain parts of this secret?</h3>
                        <p>
                            When validators rotate in/out, they generate new shares of the Network Key (derived from their share) and collaborate
                            with other validators to generate a new dealing of the shared secret that can still generate valid signatures for the
                            same Network Key.
                        </p>
                        <p>
                            The Commonware Library <a href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/dkg/index.html">provides an implementation</a> for performing a synchronous DKG/Resharing but you can bring whatever construction you'd like (it isn't enshrined
                            into alto)!
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

export default KeyInfoModal;