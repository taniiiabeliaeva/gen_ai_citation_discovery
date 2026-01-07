import React, { useState, useEffect } from 'react';

export default function DocumentExplorer({ onDocumentSelect, selectedDocIds = [], onDocumentView, viewedDocument, relevanceScores = {}, sortBy = 'name', onSortChange, onDocumentsChange }) {
    const [documents, setDocuments] = useState([]);
    const [isUploading, setIsUploading] = useState(false);
    const [uploadProgress, setUploadProgress] = useState('');

    useEffect(() => {
        fetchDocuments();
    }, []);

    const fetchDocuments = async () => {
        try {
            const response = await fetch('http://127.0.0.1:8000/api/documents');
            const data = await response.json();
            const docs = data.documents || [];
            setDocuments(docs);
            if (onDocumentsChange) {
                onDocumentsChange(docs);
            }
        } catch (error) {
            console.error('Error fetching documents:', error);
        }
    };

    const handleFileUpload = async (event) => {
        const file = event.target.files?.[0];
        if (!file) return;

        if (!file.name.endsWith('.pdf')) {
            alert('Please upload a PDF file');
            return;
        }

        setIsUploading(true);
        setUploadProgress('Uploading...');

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('http://127.0.0.1:8000/api/documents/upload', {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                throw new Error('Upload failed');
            }

            setUploadProgress('Processing...');
            await fetchDocuments();
            setUploadProgress('');
            event.target.value = ''; // Reset file input
        } catch (error) {
            console.error('Error uploading document:', error);
            alert('Failed to upload document');
            setUploadProgress('');
        } finally {
            setIsUploading(false);
        }
    };

    const handleDelete = async (filePath) => {
        if (!confirm('Are you sure you want to delete this document?')) return;

        try {
            const response = await fetch(`http://127.0.0.1:8000/api/documents/${encodeURIComponent(filePath)}`, {
                method: 'DELETE',
            });

            if (response.ok) {
                await fetchDocuments();
            } else {
                alert('Failed to delete document');
            }
        } catch (error) {
            console.error('Error deleting document:', error);
            alert('Failed to delete document');
        }
    };

    const toggleDocumentSelection = (filePath) => {
        const newSelection = selectedDocIds.includes(filePath)
            ? selectedDocIds.filter(id => id !== filePath)
            : [...selectedDocIds, filePath];
        onDocumentSelect(newSelection);
    };

    const isDocumentViewed = (doc) => {
        return viewedDocument && viewedDocument.file_path === doc.file_path;
    };

    const getRelevanceScore = (filePath) => {
        return relevanceScores[filePath] || 0;
    };

    const getRelevanceBadge = (score) => {
        if (score >= 0.8) return { color: 'bg-green-500', label: 'High', emoji: '🟢' };
        if (score >= 0.5) return { color: 'bg-yellow-500', label: 'Medium', emoji: '🟡' };
        if (score > 0) return { color: 'bg-gray-500', label: 'Low', emoji: '⚪' };
        return null;
    };

    const getSortedDocuments = () => {
        const docs = [...documents];
        if (sortBy === 'relevance') {
            return docs.sort((a, b) => {
                const scoreA = getRelevanceScore(a.file_path);
                const scoreB = getRelevanceScore(b.file_path);
                return scoreB - scoreA; // Descending order
            });
        }
        return docs; // Default: by name (as returned from API)
    };

    const sortedDocuments = getSortedDocuments();

    return (
        <div className="w-80 bg-gray-800 border-r border-gray-700 flex flex-col h-full">
            {/* Header */}
            <div className="p-4 border-b border-gray-700">
                <div className="flex items-center justify-between mb-3">
                    <h2 className="text-lg font-semibold text-gray-100">Documents</h2>
                    
                    {/* Sort Dropdown */}
                    {Object.keys(relevanceScores).length > 0 && (
                        <select
                            value={sortBy}
                            onChange={(e) => onSortChange(e.target.value)}
                            className="text-xs bg-gray-700 text-gray-300 border border-gray-600 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        >
                            <option value="name">Sort by Name</option>
                            <option value="relevance">Sort by Relevance</option>
                        </select>
                    )}
                </div>

                {/* Upload Button */}
                <label className="block">
                    <input
                        type="file"
                        accept=".pdf"
                        onChange={handleFileUpload}
                        disabled={isUploading}
                        className="hidden"
                    />
                    <div className="w-full bg-blue-600 hover:bg-blue-500 text-white rounded-lg px-4 py-2.5 text-sm font-medium cursor-pointer text-center transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
                        {isUploading ? uploadProgress : '+ Upload PDF'}
                    </div>
                </label>
            </div>

            {/* Document List */}
            <div className="flex-1 overflow-y-auto p-3 space-y-2">
                {sortedDocuments.length === 0 ? (
                    <div className="text-center text-gray-500 text-sm mt-8">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-12 h-12 mx-auto mb-2 opacity-50">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                        </svg>
                        No documents yet.<br />Upload a PDF to get started.
                    </div>
                ) : (
                    sortedDocuments.map((doc) => {
                        const score = getRelevanceScore(doc.file_path);
                        const badge = getRelevanceBadge(score);

                        return (
                            <div
                                key={doc.file_path}
                                className={`group bg-gray-750 hover:bg-gray-700 border rounded-lg p-3 cursor-pointer transition-all ${isDocumentViewed(doc)
                                        ? 'border-blue-400 bg-blue-900/20'
                                        : selectedDocIds.includes(doc.file_path)
                                            ? 'border-blue-500 bg-gray-700'
                                            : 'border-gray-700'
                                    }`}
                                onClick={() => toggleDocumentSelection(doc.file_path)}
                            >
                                <div className="flex items-start gap-2">
                                    {/* Checkbox */}
                                    <div className="mt-0.5">
                                        <div className={`w-4 h-4 rounded border-2 flex items-center justify-center ${selectedDocIds.includes(doc.file_path)
                                                ? 'bg-blue-600 border-blue-600'
                                                : 'border-gray-600'
                                            }`}>
                                            {selectedDocIds.includes(doc.file_path) && (
                                                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" className="w-3 h-3 text-white">
                                                    <path fillRule="evenodd" d="M19.916 4.626a.75.75 0 01.208 1.04l-9 13.5a.75.75 0 01-1.154.114l-6-6a.75.75 0 011.06-1.06l5.353 5.353 8.493-12.739a.75.75 0 011.04-.208z" clipRule="evenodd" />
                                                </svg>
                                            )}
                                        </div>
                                    </div>

                                    {/* Document Info */}
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2">
                                            <div className="text-sm font-medium text-gray-200 flex-1" title={doc.title}>
                                                {doc.title || 'Untitled'}
                                            </div>
                                            {/* Relevance Badge */}
                                            {badge && (
                                                <div className="flex items-center gap-1">
                                                    <span className="text-sm">{badge.emoji}</span>
                                                    <span className="text-xs text-gray-400">{score.toFixed(2)}</span>
                                                </div>
                                            )}
                                        </div>
                                        <div className="flex items-center justify-between gap-2 mt-1">
                                            <div className="text-xs text-gray-400 flex items-center gap-2">
                                                <span>{doc.total_pages} page{doc.total_pages !== 1 ? 's' : ''}</span>
                                                {doc.cited_by_count !== "" && (
                                                    <span>• {doc.cited_by_count} citations</span>
                                                )}
                                            </div>
                                            
                                            {/* Action Buttons - Smaller and inline */}
                                            <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                                                {/* View Button */}
                                                <button
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        onDocumentView(doc);
                                                    }}
                                                    className="p-0.5 hover:bg-blue-600/20 rounded transition-all"
                                                    title="View PDF"
                                                >
                                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-3.5 h-3.5 text-blue-400">
                                                        <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
                                                        <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                                                    </svg>
                                                </button>

                                                {/* Delete Button */}
                                                <button
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        handleDelete(doc.file_path);
                                                    }}
                                                    className="p-0.5 hover:bg-red-600/20 rounded transition-all"
                                                    title="Delete document"
                                                >
                                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-3.5 h-3.5 text-red-400">
                                                        <path strokeLinecap="round" strokeLinejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                                                    </svg>
                                                </button>
                                            </div>
                                        </div>
                                    </div>


                                </div>
                            </div>
                        );
                    })
                )}
            </div>

            {/* Selected Count */}
            {selectedDocIds.length > 0 && (
                <div className="p-3 border-t border-gray-700 bg-gray-800">
                    <div className="flex items-center justify-between gap-2">
                        <div className="text-xs text-gray-400">
                            {selectedDocIds.length} document{selectedDocIds.length !== 1 ? 's' : ''} selected
                        </div>
                        <button
                            onClick={() => onDocumentSelect([])}
                            className="text-xs text-blue-400 hover:text-blue-300 font-medium transition-colors"
                        >
                            Unselect All
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
