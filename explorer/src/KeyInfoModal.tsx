import React, { useEffect } from 'react';

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
            const modalLinks = document.querySelectorAll('.key-info-modal a');
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
            <div className="about-modal key-info-modal">
                <div className="about-modal-header">
                    <h2>Network Key Information</h2>
                </div>
                <div className="about-modal-content">
                    <section>
                        <h3>What is the Network Key?</h3>
                        <p>
                            This key is a BLS12-381 public key that represents the threshold signature of the validator set.
                            It's used to verify all consensus messages received by your browser.
                        </p>
                    </section>

                    <section>
                        <h3>How Message Verification Works</h3>
                        <p>
                            When your browser receives a consensus message (seed, notarization, or finalization):
                        </p>
                        <ol>
                            <li>The message arrives containing a BLS signature</li>
                            <li>Your browser uses WebAssembly to verify this signature against the network key</li>
                            <li>If the signature is valid, the message is processed and displayed</li>
                            <li>If invalid, the message is rejected</li>
                        </ol>
                        <p>
                            This creates a trustless system where you don't need to trust our infrastructure.
                            Your browser directly verifies that each message was produced by the consensus set.
                        </p>
                    </section>

                    <section>
                        <h3>The Network Key</h3>
                        <div className="key-display-box">
                            <code>{publicKeyHex}</code>
                            <button
                                className="copy-key-button"
                                onClick={() => {
                                    navigator.clipboard.writeText(publicKeyHex);
                                    const button = document.querySelector('.copy-key-button');
                                    if (button) {
                                        button.textContent = "Copied!";
                                        setTimeout(() => {
                                            button.textContent = "Copy";
                                        }, 2000);
                                    }
                                }}
                            >
                                Copy
                            </button>
                        </div>
                    </section>

                    <section>
                        <h3>Technical Details</h3>
                        <p>
                            The BLS12-381 signature scheme allows for threshold signatures, where a subset of validators (at least 2f+1 out of 3f+1)
                            can produce a valid signature. This is what enables the Byzantine fault tolerance of the consensus system.
                        </p>
                        <p>
                            The WASM module used for verification (<code>alto_types.js</code>) efficiently verifies these signatures on the client side,
                            eliminating the need to trust the server for correctness.
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