import React, { useEffect, useState } from 'react';
import './AboutModal.css'; // Sharing the same CSS file
import init, { sha256 } from './alto_types/alto_types.js';
import { ClusterConfig } from './config';
import { decodeVarintPrefix, hexToUint8Array, hexUint8Array } from './utils';

interface KeyInfoModalProps {
    isOpen: boolean;
    onClose: () => void;
    clusterConfig: ClusterConfig;
}

const KeyInfoModal: React.FC<KeyInfoModalProps> = ({ isOpen, onClose, clusterConfig }) => {
    const publicKeyHex = clusterConfig.PUBLIC_KEY_HEX;
    const [identityDigest, setIdentityDigest] = useState<{ hex: string; digest: string } | null>(null);

    // The participant set is too large to display, so summarize it by its SHA-256 digest. The WASM
    // hash avoids crypto.subtle, which is unavailable on plain-http deployments.
    useEffect(() => {
        if (!isOpen) {
            return;
        }
        let cancelled = false;
        const digestIdentity = async () => {
            let digest: string;
            try {
                await init();
                digest = hexUint8Array(sha256(hexToUint8Array(publicKeyHex)), 64);
            } catch (error) {
                console.error('Unable to hash the participant set:', error);
                digest = 'Unavailable';
            }
            if (!cancelled) {
                setIdentityDigest({ hex: publicKeyHex, digest });
            }
        };
        digestIdentity();
        return () => {
            cancelled = true;
        };
    }, [isOpen, publicKeyHex]);

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

    let participants: number | null = null;
    try {
        participants = decodeVarintPrefix(hexToUint8Array(publicKeyHex));
    } catch {
        // Malformed hex leaves the count unknown.
    }
    const digest = identityDigest?.hex === publicKeyHex ? identityDigest.digest : null;
    const content = (
        <>
            <section>
                <h3>I'm verifying post-quantum signatures?</h3>
                <p>
                    Consensus messages (notarizations and finalizations) carry <i>FN-DSA-512</i> (Falcon) signatures, one from each of
                    the <strong>2f+1</strong> validators in the quorum. Your browser uses <a href="https://github.com/commonwarexyz/monorepo/blob/1950760f8bc64f6d0c45bef3d68c0947c94284b2/cryptography/src/fn_dsa/mod.rs">cryptography::fn_dsa</a> (compiled
                    to WebAssembly) to verify every signature against this cluster's <strong>Participant Set</strong> before updating the timeline.
                </p>
                <p>
                    Messages that fail verification are ignored. If processing falls behind, older messages may be skipped.
                </p>
            </section>

            <section>
                <h3>What is a Participant Set?</h3>
                <p>
                    The Participant Set is the ordered list of validator identity keys: the same <i>FN-DSA-512</i> public keys validators
                    use to sign consensus messages. There is no shared Network Key and no DKG.
                </p>
                <p>
                    The <strong>Participant Set</strong> for alto contains <strong>{participants ?? 'an unknown number of'}</strong> validators. Its SHA-256 digest is:
                </p>
                <pre className="code-block">
                    <code>{digest ?? 'Computing...'}</code>
                </pre>
                <p>
                    <a href={`data:text/plain;charset=utf-8,${publicKeyHex}`} download="identity.hex">Download identity.hex</a> for
                    the full encoded Participant Set.
                </p>
                <p>
                    <i>
                        In a production environment, you would hardcode this in your binary or store it locally rather than relying
                        on a website to provide it (like with <a href="https://github.com/commonwarexyz/alto/tree/pq/inspector">alto-inspector</a>).
                    </i>
                </p>
            </section>
            <section>
                <h3>Can validators be rotated?</h3>
                <p>
                    Each signature is attributable to one validator, so changing the validators changes the Participant Set (and its
                    digest). Verifiers must adopt the new set to accept certificates signed by the new validators.
                </p>
            </section>
        </>
    );

    return (
        <div className="about-modal-overlay">
            <div className="about-modal">
                <div className="about-modal-header">
                    <h2>Verifying Inbound Messages</h2>
                </div>
                <div className="about-modal-content">
                    {content}
                </div>
                <div className="about-modal-footer">
                    <button className="about-button" onClick={onClose}>Close</button>
                </div>
            </div>
        </div>
    );
};

export default KeyInfoModal;
