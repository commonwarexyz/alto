import React, { useState, useEffect } from 'react';
import './SearchModal.css';
import { BACKEND_URL, PUBLIC_KEY_HEX } from './config';
import { FinalizedJs, NotarizedJs, BlockJs, SearchType, SearchResult } from './types';
import { hexToUint8Array, hexUint8Array, formatAge } from './utils';
import init, { parse_seed, parse_notarized, parse_finalized, parse_block } from "./alto_types/alto_types.js";

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
    const [lastSearchType, setLastSearchType] = useState<SearchType | null>(null);
    const [showHelp, setShowHelp] = useState<boolean>(false);
    const [wasmInitialized, setWasmInitialized] = useState<boolean>(false);

    // Initialize WASM module on component mount
    useEffect(() => {
        const initWasm = async () => {
            try {
                await init();
                setWasmInitialized(true);
            } catch (error) {
                console.error("Failed to initialize WASM module:", error);
                setError("Failed to initialize search functionality. Please try again later.");
            }
        };

        if (!wasmInitialized) {
            initWasm();
        }
    }, [wasmInitialized]);

    // Helper function to convert a number to U64 hex representation
    const numberToU64Hex = (num: number): string => {
        // Create a buffer for 8 bytes (u64)
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);

        // Set the value as a big-endian u64
        // JavaScript can only handle 53 bits precisely, but this should be sufficient
        // for our block heights and view numbers
        view.setBigUint64(0, BigInt(num), false); // false = big-endian

        // Convert to hex string
        return Array.from(new Uint8Array(buffer))
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
    };

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
        // Store the current search type
        setLastSearchType(searchType);

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

                // Limit range size to prevent too many requests
                const maxRangeSize = 20;
                const actualEnd = Math.min(end, start + maxRangeSize - 1);

                // Clear any existing results before starting a new search
                setResults([]);
                let foundAnyResults = false;

                // Process each index in the range sequentially
                for (let i = start; i <= actualEnd; i++) {
                    try {
                        const result = await fetchSingleItem(i);
                        if (result) {
                            // Immediately add this result to the results array
                            setResults(prevResults => [...prevResults, result]);
                            foundAnyResults = true;
                        }
                    } catch (err) {
                        console.error(`Error fetching ${searchType} at index ${i}:`, err);
                    }
                }

                // After trying all indices, if we didn't find anything, show an error
                if (!foundAnyResults) {
                    setError(`No results found for range ${start}..${actualEnd}`);
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
        if (!wasmInitialized) {
            throw new Error("Search functionality is still initializing. Please try again in a moment.");
        }

        const baseUrl = `https://${BACKEND_URL}`;
        const PUBLIC_KEY = hexToUint8Array(PUBLIC_KEY_HEX);

        let endpoint = '';

        // Match endpoint patterns from client.rs
        switch (searchType) {
            case 'block':
                endpoint = `/block/${typeof query === 'number' ? numberToU64Hex(query) : query}`;
                break;
            case 'notarization':
                endpoint = `/notarization/${typeof query === 'number' ? numberToU64Hex(query) : query}`;
                break;
            case 'finalization':
                endpoint = `/finalization/${typeof query === 'number' ? numberToU64Hex(query) : query}`;
                break;
            case 'seed':
                endpoint = `/seed/${typeof query === 'number' ? numberToU64Hex(query) : query}`;
                break;
        }

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

            // Use proper parsing functions from the WASM module
            try {
                if (searchType === 'seed') {
                    // Parse seed data directly without slicing
                    const result = parse_seed(PUBLIC_KEY, data);
                    if (!result) throw new Error("Failed to parse seed data");
                    return result;
                } else if (searchType === 'notarization') {
                    // Parse notarization data directly without slicing
                    const result = parse_notarized(PUBLIC_KEY, data);
                    if (!result) throw new Error("Failed to parse notarization data");
                    return result;
                } else if (searchType === 'finalization') {
                    // Parse finalization data directly without slicing
                    const result = parse_finalized(PUBLIC_KEY, data);
                    if (!result) throw new Error("Failed to parse finalization data");
                    return result;
                } else if (searchType === 'block') {
                    if (query === 'latest') {
                        // For 'latest' query, we get a finalized object
                        const result = parse_finalized(PUBLIC_KEY, data);
                        if (!result) throw new Error("Failed to parse latest block data");
                        return result;
                    } else if (typeof query === 'number') {
                        // For index queries, we also get a finalized object
                        const result = parse_finalized(PUBLIC_KEY, data);
                        if (!result) throw new Error("Failed to parse block data by height");
                        return result;
                    } else {
                        // For digest queries, we get a plain block
                        const result = parse_block(data);
                        if (!result) throw new Error("Failed to parse block data by digest");
                        return result;
                    }
                }
            } catch (parseError) {
                console.error(`Error parsing ${searchType} data:`, parseError);
                const errorMessage = parseError instanceof Error
                    ? parseError.message
                    : String(parseError);
                throw new Error(`Failed to parse ${searchType} data: ${errorMessage}`);
            }

            // Fallback: return raw data (should not normally reach here)
            console.warn(`Unexpected data format for ${searchType}, returning raw data`);
            return data as any;
        } catch (error) {
            console.error(`Error fetching ${searchType}:`, error);
            throw error;
        }
    };

    const renderSearchResult = (result: SearchResult, index: number) => {
        if (!result) return null;

        // Format the result based on its type
        let formattedResult: Record<string, any> = {};
        let resultType;

        // Attempt to determine the type of the result
        if ('view' in result && 'signature' in result && !('proof' in result)) {
            // This is a Seed object
            resultType = 'Seed';
            formattedResult = {
                view: result.view,
                signature: hexUint8Array(result.signature as Uint8Array, 64) // Show full signature
            };
        } else if ('proof' in result && 'block' in result) {
            // This is either a Notarization or Finalization object

            // Determine the type based on the search type that was used for the query
            if (lastSearchType === 'finalization') {
                resultType = 'Finalization';
            } else if (lastSearchType === 'notarization') {
                resultType = 'Notarization';
            } else if (lastSearchType === 'block') {
                // For block searches, always show as Finalization if it has a proof
                resultType = 'Finalization';
            } else {
                // Fallback detection if lastSearchType is null
                resultType = 'quorum' in result ? 'Finalization' : 'Notarization';
            }

            const dataObj = result as (NotarizedJs | FinalizedJs);
            const block = dataObj.block;
            const now = Date.now();
            const age = now - Number(block.timestamp);

            formattedResult = {
                view: dataObj.proof.view,
                height: block.height,
                timestamp: new Date(Number(block.timestamp)).toLocaleString(),
                age: formatAge(age),
                digest: hexUint8Array(block.digest as Uint8Array, 64),
                parent: hexUint8Array(block.parent, 64)
            };

            // Add signature if available
            if (dataObj.proof.signature) {
                formattedResult.signature = hexUint8Array(dataObj.proof.signature, 64); // Show full signature
            }

        } else if ('height' in result && 'timestamp' in result && 'digest' in result) {
            // This is a Block object
            resultType = 'Block';
            const block = result as BlockJs;
            const now = Date.now();
            const age = now - Number(block.timestamp);

            formattedResult = {
                height: block.height,
                timestamp: new Date(Number(block.timestamp)).toLocaleString(),
                age: formatAge(age),
                digest: hexUint8Array(block.digest as Uint8Array, 64),
                parent: hexUint8Array(block.parent, 64)
            };
        } else {
            // Unknown or unrecognized data structure
            resultType = 'Unknown Data';
            formattedResult = {
                raw: JSON.stringify(result, (key, value) => {
                    if (value && value.constructor === Uint8Array) {
                        return hexUint8Array(value as Uint8Array, 64); // Show full binary data
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
            if (key === 'parent') return `${baseClass} digest`;
            return baseClass;
        };

        return (
            <div key={index} className="search-result-item">
                <div className="search-result-header">
                    <strong>{resultType}</strong>
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
        if (!wasmInitialized) {
            setError("Search functionality is still initializing. Please try again in a moment.");
            return;
        }
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
                                    <option value="seed">Seed</option>
                                    <option value="notarization">Notarization</option>
                                    <option value="finalization">Finalization</option>
                                    <option value="block">Block</option>
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
                                disabled={isLoading || !wasmInitialized}
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
                        <h3>Results {isLoading && <span className="search-loading-indicator">(Loading...)</span>}</h3>
                        {results.length > 0 ? (
                            <div className="search-result-list">
                                {results.map((result, index) => renderSearchResult(result, index))}
                            </div>
                        ) : (
                            !error && (
                                <div className="search-no-results">
                                    {isLoading ? "Searching..." : "No results to display. Try a different search."}
                                </div>
                            )
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