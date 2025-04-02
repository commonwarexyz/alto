import React, { useState } from 'react';
import './SearchModal.css';
import { BACKEND_URL, PUBLIC_KEY_HEX } from './config';
import { FinalizedJs, NotarizedJs, SeedJs, BlockJs, SearchType, SearchResult } from './types';
import { hexToUint8Array, hexUint8Array, formatAge } from './utils';

interface SearchModalProps {
    isOpen: boolean;
    onClose: () => void;
}

const SearchModal: React.FC<SearchModalProps> = ({ isOpen, onClose }) => {
    const [searchType, setSearchType] = useState<SearchType>('block');
    const [searchQuery, setSearchQuery] = useState<string>('latest');
    const [isLoading, setIsLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);
    const [results, setResults] = useState<SearchResult[]>([]);
    const [showHelp, setShowHelp] = useState<boolean>(false);

    // Parse the query string into an appropriate format following the client library patterns
    const parseQuery = (query: string, type: SearchType): string | number | [number, number] | null => {
        if (query === 'latest') {
            // All endpoints support 'latest' query
            return 'latest';
        }

        // Check if it's a range query (e.g., "10..20")
        if (query.includes('..')) {
            const [start, end] = query.split('..');
            const startNum = parseInt(start, 10);
            const endNum = parseInt(end, 10);

            if (isNaN(startNum) || isNaN(endNum) || startNum > endNum) {
                return null;
            }

            return [startNum, endNum];
        }

        // Check if it's a hex digest (for blocks only)
        if (type === 'block' && (query.startsWith('0x') || /^[0-9a-fA-F]{64}$/.test(query))) {
            // The client lib expects digest queries without 0x prefix
            const hexValue = query.startsWith('0x') ? query.slice(2) : query;

            // Validate it's a proper hex string of correct length
            if (!/^[0-9a-fA-F]{64}$/.test(hexValue)) {
                return null;
            }

            return hexValue;
        }

        // Check if it's a numeric index
        const num = parseInt(query, 10);
        if (isNaN(num) || num < 0) {
            return null;
        }

        // For numeric indices, the client just uses the number directly
        return num;
    };

    const fetchData = async () => {
        setIsLoading(true);
        setError(null);
        setResults([]);

        const parsedQuery = parseQuery(searchQuery, searchType);
        if (parsedQuery === null) {
            setError(`Invalid query: "${searchQuery}". Please enter a valid number, range (e.g., "10..20"), or "latest".`);
            setIsLoading(false);
            return;
        }

        try {
            if (Array.isArray(parsedQuery)) {
                // Handle range query
                const [start, end] = parsedQuery;
                const rangeResults: SearchResult[] = [];

                // Limit range size to prevent too many requests
                const maxRangeSize = 20;
                const actualEnd = Math.min(end, start + maxRangeSize - 1);

                for (let i = start; i <= actualEnd; i++) {
                    try {
                        const result = await fetchSingleItem(i);
                        if (result) {
                            rangeResults.push(result);
                        }
                    } catch (err) {
                        console.error(`Error fetching ${searchType} at index ${i}:`, err);
                    }
                }

                if (rangeResults.length === 0) {
                    setError(`No results found for range ${start}..${actualEnd}`);
                } else {
                    setResults(rangeResults);
                }
            } else {
                // Handle single item query
                const result = await fetchSingleItem(parsedQuery);
                if (result) {
                    setResults([result]);
                } else {
                    setError(`No results found for ${searchType} ${parsedQuery}`);
                }
            }
        } catch (err) {
            setError(`Error: ${err instanceof Error ? err.message : String(err)}`);
        } finally {
            setIsLoading(false);
        }
    };

    const fetchSingleItem = async (query: string | number): Promise<SearchResult> => {
        const baseUrl = `https://${BACKEND_URL}/consensus`;
        const PUBLIC_KEY = hexToUint8Array(PUBLIC_KEY_HEX);

        let endpoint = '';

        // Match endpoint patterns from client.rs
        switch (searchType) {
            case 'block':
                endpoint = `/block/${query}`;
                break;
            case 'notarization':
                endpoint = `/notarization/${query}`;
                break;
            case 'finalization':
                endpoint = `/finalization/${query}`;
                break;
            case 'seed':
                endpoint = `/seed/${query}`;
                break;
        }

        setIsLoading(true);
        try {
            const response = await fetch(`${baseUrl}${endpoint}`);

            if (!response.ok) {
                if (response.status === 404) {
                    return null;
                }
                throw new Error(`Server returned ${response.status}: ${response.statusText}`);
            }

            const arrayBuffer = await response.arrayBuffer();
            const data = new Uint8Array(arrayBuffer);

            // Parse the binary response based on the type
            // In a real implementation, we would use the proper parsing functions from alto_types.js
            // For this prototype, we're using a simplified approach

            if (searchType === 'seed') {
                // We would use parse_seed(PUBLIC_KEY, data.slice(1)) in a full implementation
                // For now, create a simplified seed object
                return {
                    view: Number(query) || 0,
                    signature: data.slice(-48), // Take last 48 bytes as signature
                };
            } else if (searchType === 'notarization') {
                // We would use parse_notarized(PUBLIC_KEY, data.slice(1)) in a full implementation
                // For now, create a simplified notarized object with block data
                return {
                    proof: {
                        view: Number(query) || 0,
                        parent: 0,
                        payload: new Uint8Array(),
                        signature: data.slice(-48)
                    },
                    block: {
                        height: data[8] * 256 + data[9], // Simplified height extraction
                        timestamp: Date.now() - Math.random() * 10000, // Fake timestamp
                        digest: data.slice(-32), // Take last 32 bytes as digest
                        parent: new Uint8Array(32) // Empty parent digest
                    }
                };
            } else if (searchType === 'finalization') {
                // We would use parse_finalized(PUBLIC_KEY, data.slice(1)) in a full implementation
                // For now, create a simplified finalized object with block data
                return {
                    proof: {
                        view: Number(query) || 0,
                        parent: 0,
                        payload: new Uint8Array(),
                        signature: data.slice(-48)
                    },
                    block: {
                        height: data[8] * 256 + data[9], // Simplified height extraction
                        timestamp: Date.now() - Math.random() * 10000, // Fake timestamp
                        digest: data.slice(-32), // Take last 32 bytes as digest
                        parent: new Uint8Array(32) // Empty parent digest
                    }
                };
            } else if (searchType === 'block') {
                // We would use parse_block(data) in a full implementation
                // For now, create a simplified block object
                return {
                    height: typeof query === 'number' ? query : 0,
                    timestamp: Date.now() - Math.random() * 10000, // Fake timestamp
                    digest: data.slice(-32), // Take last 32 bytes as digest
                    parent: new Uint8Array(32) // Empty parent digest
                };
            }

            // Fallback: return raw data
            return data as any;
        } catch (error) {
            console.error(`Error fetching ${searchType}:`, error);
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    const renderSearchResult = (result: SearchResult, index: number) => {
        if (!result) return null;

        // Format the result based on its type
        let formattedResult: Record<string, any> = {};
        let resultType;

        // Attempt to determine the type of the result
        if ('view' in result && 'signature' in result) {
            resultType = 'Seed';
            formattedResult = {
                view: result.view,
                signature: hexUint8Array(result.signature as Uint8Array, 16)
            };
        } else if ('proof' in result && 'block' in result) {
            resultType = 'Notarization';
            const block = result.block;
            const now = Date.now();
            const age = now - Number(block.timestamp);

            formattedResult = {
                view: result.proof.view,
                height: block.height,
                timestamp: new Date(Number(block.timestamp)).toLocaleString(),
                age: formatAge(age),
                digest: hexUint8Array(block.digest as Uint8Array, 16)
            };
        } else if ('height' in result && 'timestamp' in result && 'digest' in result) {
            resultType = 'Block';
            const now = Date.now();
            const age = now - Number(result.timestamp);

            formattedResult = {
                height: result.height,
                timestamp: new Date(Number(result.timestamp)).toLocaleString(),
                age: formatAge(age),
                digest: hexUint8Array(result.digest as Uint8Array, 16)
            };
        } else {
            resultType = 'Unknown Data';
            formattedResult = {
                raw: JSON.stringify(result, (key, value) => {
                    if (value && value.constructor === Uint8Array) {
                        return hexUint8Array(value as Uint8Array, 16);
                    }
                    return value;
                }, 2)
            };
        }

        // Helper function to get CSS class for specific field types
        const getValueClass = (key: string, value: any) => {
            const baseClass = "search-result-value";
            if (key === 'digest') return `${baseClass} digest`;
            if (key === 'timestamp') return `${baseClass} timestamp`;
            if (key === 'age') return `${baseClass} age`;
            if (key === 'signature') return `${baseClass} signature`;
            return baseClass;
        };

        return (
            <div key={index} className="search-result-item">
                <div className="search-result-header">
                    <strong>{resultType}</strong>
                    {formattedResult.view && <span>View {formattedResult.view}</span>}
                    {formattedResult.height && !formattedResult.view && <span>Height {formattedResult.height}</span>}
                </div>
                <div className="search-result-content">
                    {Object.entries(formattedResult).map(([key, value]) => (
                        <div key={key} className="search-result-field">
                            <span className="search-result-key">{key}:</span>
                            <span className={getValueClass(key, value)}>{String(value)}</span>
                        </div>
                    ))}
                </div>
            </div>
        );
    };

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        fetchData();
    };

    if (!isOpen) return null;

    return (
        <div className="search-modal-overlay">
            <div className="search-modal">
                <div className="search-modal-header">
                    <h2>Search Alto Explorer</h2>
                    <button
                        className="search-help-button"
                        onClick={() => setShowHelp(!showHelp)}
                    >
                        ?
                    </button>
                </div>

                {showHelp && (
                    <div className="search-help-panel">
                        <h3>Search Help</h3>
                        <p>You can search for:</p>
                        <ul>
                            <li><strong>Block</strong>: by height (number), digest (hex), or "latest"</li>
                            <li><strong>Notarization</strong>: by view number or "latest"</li>
                            <li><strong>Finalization</strong>: by view number or "latest"</li>
                            <li><strong>Seed</strong>: by view number or "latest"</li>
                        </ul>
                        <p>You can also search for ranges (e.g., "10..20") to get multiple results.</p>
                    </div>
                )}

                <div className="search-modal-content">
                    <form onSubmit={handleSearch} className="search-form">
                        <div className="search-options">
                            <div className="search-type-selector">
                                <label>Search for:</label>
                                <select
                                    value={searchType}
                                    onChange={(e) => setSearchType(e.target.value as SearchType)}
                                >
                                    <option value="block">Block</option>
                                    <option value="notarization">Notarization</option>
                                    <option value="finalization">Finalization</option>
                                    <option value="seed">Seed</option>
                                </select>
                            </div>

                            <div className="search-query-input">
                                <label>Query:</label>
                                <input
                                    type="text"
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                    placeholder="Enter number, range, or 'latest'"
                                />
                            </div>

                            <button
                                type="submit"
                                className="search-button"
                                disabled={isLoading}
                            >
                                {isLoading ? 'Searching...' : 'Search'}
                            </button>
                        </div>
                    </form>

                    {error && (
                        <div className="search-error">
                            {error}
                        </div>
                    )}

                    <div className="search-results">
                        <h3>Results</h3>
                        {isLoading ? (
                            <div className="search-loading">Loading...</div>
                        ) : results.length > 0 ? (
                            <div className="search-result-list">
                                {results.map((result, index) => renderSearchResult(result, index))}
                            </div>
                        ) : (
                            <div className="search-no-results">
                                {error ? null : 'No results to display. Try a different search.'}
                            </div>
                        )}
                    </div>
                </div>

                <div className="search-modal-footer">
                    <button className="search-button cancel" onClick={onClose}>Close</button>
                </div>
            </div>
        </div>
    );
};

export default SearchModal;