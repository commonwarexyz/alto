import React, { useState, useEffect } from 'react';
import './SearchModal.css';
import { ClusterConfig, getHttpBackendUrl } from './config';
import { SearchType, SearchResult } from './types';
import { hexToUint8Array, hexUint8Array, formatAge } from './utils';
import init, { parse_seed, parse_notarized, parse_finalized, parse_block } from "./alto_types/alto_types.js";

interface SearchModalProps {
    isOpen: boolean;
    onClose: () => void;
    clusterConfig: ClusterConfig;
}

interface SearchResultWithLatency {
    result: SearchResult;
    latency: number;
}

const SearchModal: React.FC<SearchModalProps> = ({ isOpen, onClose, clusterConfig }) => {
    const [searchType, setSearchType] = useState<SearchType>('finalization');
    const [searchQuery, setSearchQuery] = useState<string>('42645..42664');
    const [isLoading, setIsLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);
    const [results, setResults] = useState<SearchResultWithLatency[]>([]);
    const [lastSearchType, setLastSearchType] = useState<SearchType | null>(null);
    const [showHelp, setShowHelp] = useState<boolean>(false);
    const [wasmInitialized, setWasmInitialized] = useState<boolean>(false);
    const standardCertificates = clusterConfig.CERTIFICATE_MODE === 'standard';

    useEffect(() => {
        if (standardCertificates && searchType === 'seed') {
            setSearchType('finalization');
        }
    }, [searchType, standardCertificates]);

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

    const numberToU64Hex = (num: number): string => {
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        view.setBigUint64(0, BigInt(num), false);
        return Array.from(new Uint8Array(buffer))
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
    };

    const parseQuery = (query: string, type: SearchType): string | number | [number, number] | null => {
        if (query === 'latest') {
            return 'latest';
        }
        if (query.includes('..')) {
            const [start, end] = query.split('..');
            const startNum = parseInt(start, 10);
            const endNum = parseInt(end, 10);
            if (isNaN(startNum) || isNaN(endNum) || startNum > endNum) {
                return null;
            }
            return [startNum, endNum];
        }
        if (type === 'block' && (query.startsWith('0x') || /^[0-9a-fA-F]{64}$/.test(query))) {
            const hexValue = query.startsWith('0x') ? query.slice(2) : query;
            if (!/^[0-9a-fA-F]{64}$/.test(hexValue)) {
                return null;
            }
            return hexValue;
        }
        const num = parseInt(query, 10);
        if (isNaN(num) || num < 0) {
            return null;
        }
        return num;
    };

    const fetchData = async () => {
        setIsLoading(true);
        setError(null);
        setResults([]);
        setLastSearchType(searchType);

        const parsedQuery = parseQuery(searchQuery, searchType);
        if (parsedQuery === null) {
            setError(`Invalid query: "${searchQuery}". Please enter a valid number, range (e.g., "10..20"), or "latest".`);
            setIsLoading(false);
            return;
        }

        try {
            if (Array.isArray(parsedQuery)) {
                const [start, end] = parsedQuery;
                const maxRangeSize = 20;
                const actualEnd = Math.min(end, start + maxRangeSize - 1);
                setResults([]);
                let foundAnyResults = false;

                for (let i = start; i <= actualEnd; i++) {
                    try {
                        const startTime = performance.now();
                        const result = await fetchSingleItem(i);
                        const endTime = performance.now();
                        const latency = Math.round(endTime - startTime);
                        if (result) {
                            setResults(prevResults => [...prevResults, { result, latency }]);
                            foundAnyResults = true;
                        }
                    } catch (err) {
                        console.error(`Error fetching ${searchType} at index ${i}:`, err);
                    }
                }

                if (!foundAnyResults) {
                    setError(`No results found for range ${start}..${actualEnd}`);
                }
            } else {
                const startTime = performance.now();
                const result = await fetchSingleItem(parsedQuery);
                const endTime = performance.now();
                const latency = Math.round(endTime - startTime);
                if (result) {
                    setResults([{ result, latency }]);
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

    const fetchSingleItem = async (query: string | number): Promise<SearchResult | null> => {
        if (!wasmInitialized) {
            throw new Error("Search functionality is still initializing. Please try again in a moment.");
        }

        const { BACKEND_URL, PUBLIC_KEY_HEX } = clusterConfig;
        const baseUrl = getHttpBackendUrl(BACKEND_URL);
        const PUBLIC_KEY = hexToUint8Array(PUBLIC_KEY_HEX);

        const endpoint = `/${searchType}/${typeof query === 'number' ? numberToU64Hex(query) : query}`;
        const response = await fetch(`${baseUrl}${endpoint}`);
        if (!response.ok) {
            if (response.status === 404) {
                return null;
            }
            throw new Error(`Server returned ${response.status}: ${response.statusText}`);
        }

        const data = new Uint8Array(await response.arrayBuffer());
        let result: SearchResult | null;
        try {
            switch (searchType) {
                case 'seed':
                    result = parse_seed(PUBLIC_KEY, data);
                    break;
                case 'notarization':
                    result = parse_notarized(PUBLIC_KEY, data, standardCertificates);
                    break;
                case 'finalization':
                    result = parse_finalized(PUBLIC_KEY, data, standardCertificates);
                    break;
                case 'block':
                    result = query === 'latest' || typeof query === 'number'
                        ? parse_finalized(PUBLIC_KEY, data, standardCertificates)
                        : parse_block(data);
                    break;
            }
        } catch (parseError) {
            const errorMessage = parseError instanceof Error ? parseError.message : String(parseError);
            throw new Error(`Failed to parse ${searchType} data: ${errorMessage}`);
        }
        if (!result) throw new Error(`Failed to parse ${searchType} data`);
        return result;
    };

    const renderSearchResult = (item: SearchResultWithLatency, index: number) => {
        const { result, latency } = item;
        let formattedResult: Record<string, string | number>;
        let resultType;

        if ('block' in result) {
            resultType = (lastSearchType === 'finalization' || lastSearchType === 'block') ? 'Finalization' : 'Notarization';
            const block = result.block;
            const now = Date.now();
            const age = now - Number(block.timestamp);

            formattedResult = {
                height: block.height,
                parent: hexUint8Array(block.parent, 64),
                timestamp: `${new Date(Number(block.timestamp)).toLocaleString()} (${formatAge(age)})`,
                view: result.view,
                digest: hexUint8Array(block.digest, 64),
                signature: hexUint8Array(result.signature, 64),
            };
        } else if ('height' in result) {
            resultType = 'Block';
            const block = result;
            const now = Date.now();
            const age = now - Number(block.timestamp);

            formattedResult = {
                height: block.height,
                parent: hexUint8Array(block.parent, 64),
                timestamp: `${new Date(Number(block.timestamp)).toLocaleString()} (${formatAge(age)})`,
            };
        } else {
            resultType = 'Seed';
            formattedResult = {
                view: result.view,
                signature: hexUint8Array(result.signature, 64)
            };
        }

        const getValueClass = (key: string) => {
            const baseClass = "search-result-value";
            if (key === 'height') return `${baseClass} view`;
            if (key === 'view') return `${baseClass} view`;
            if (key === 'digest') return `${baseClass} digest`;
            if (key === 'timestamp') return `${baseClass} timestamp`;
            if (key === 'signature') return `${baseClass} signature`;
            if (key === 'parent') return `${baseClass} digest`;
            return baseClass;
        };

        return (
            <div key={index} className="search-result-item">
                <div className="search-result-header">
                    <strong>{resultType}</strong>
                </div>
                <div className="search-result-content">
                    {Object.entries(formattedResult).map(([key, value]) => (
                        <div key={key} className="search-result-field">
                            <span className="search-result-key">{key}:</span>
                            <span className={getValueClass(key)}>{String(value)}</span>
                        </div>
                    ))}
                    <span className="latency">Response Latency: {latency}ms</span>
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
                    <h2>Search for Artifacts</h2>
                    <button
                        className="search-help-button"
                        onClick={() => setShowHelp(!showHelp)}
                    >
                        ?
                    </button>
                </div>

                {showHelp && (
                    <div className="search-help-panel">
                        <h3>Help</h3>
                        <p>You can search for:</p>
                        <ul>
                            <li><strong>Block</strong>: by height (number), digest (hex), or "latest"</li>
                            <li><strong>Notarization</strong>: by view number or "latest"</li>
                            <li><strong>Finalization</strong>: by view number or "latest"</li>
                            {!standardCertificates && <li><strong>Seed</strong>: by view number or "latest"</li>}
                        </ul>
                        <p>You can also search for ranges (e.g., "10..20") to get multiple results.</p>
                    </div>
                )}

                <div className="search-modal-content">
                    <form onSubmit={handleSearch} className="search-form">
                        <div className="search-options">
                            <div className="search-type-selector">
                                <label>Type:</label>
                                <select
                                    value={searchType}
                                    onChange={(e) => setSearchType(e.target.value as SearchType)}
                                >
                                    {!standardCertificates && <option value="seed">Seed</option>}
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
                        <h3>Results</h3>
                        {results.length > 0 ? (
                            <div className="search-result-list">
                                {results.map((item, index) => renderSearchResult(item, index))}
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
