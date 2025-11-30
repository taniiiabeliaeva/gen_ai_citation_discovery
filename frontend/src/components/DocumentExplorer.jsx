import React, { useState, useEffect } from 'react';

export default function DocumentExplorer({ onDocumentSelect, selectedDocIds = [] }) {
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
            setDocuments(data.documents || []);
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

    const handleDelete = async (docId) => {
        if (!confirm('Are you sure you want to delete this document?')) return;

        try {
            const response = await fetch(`http://127.0.0.1:8000/api/documents/${docId}`, {
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

    const toggleDocumentSelection = (docId) => {
        const newSelection = selectedDocIds.includes(docId)
            ? selectedDocIds.filter(id => id !== docId)
            : [...selectedDocIds, docId];
        onDocumentSelect(newSelection);
    };

    return (
        <div className="w-80 bg-gray-800 border-r border-gray-700 flex flex-col h-full">
            {/* Header */}
            <div className="p-4 border-b border-gray-700">
                <h2 className="text-lg font-semibold text-gray-100 mb-3">Documents</h2>

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
                {documents.length === 0 ? (
                    <div className="text-center text-gray-500 text-sm mt-8">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-12 h-12 mx-auto mb-2 opacity-50">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                        </svg>
                        No documents yet.<br />Upload a PDF to get started.
                    </div>
                ) : (
                    documents.map((doc) => (
                        <div
                            key={doc.doc_id}
                            className={`group bg-gray-750 hover:bg-gray-700 border rounded-lg p-3 cursor-pointer transition-all ${selectedDocIds.includes(doc.doc_id)
                                    ? 'border-blue-500 bg-gray-700'
                                    : 'border-gray-700'
                                }`}
                            onClick={() => toggleDocumentSelection(doc.doc_id)}
                        >
                            <div className="flex items-start gap-2">
                                {/* Checkbox */}
                                <div className="mt-0.5">
                                    <div className={`w-4 h-4 rounded border-2 flex items-center justify-center ${selectedDocIds.includes(doc.doc_id)
                                            ? 'bg-blue-600 border-blue-600'
                                            : 'border-gray-600'
                                        }`}>
                                        {selectedDocIds.includes(doc.doc_id) && (
                                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" className="w-3 h-3 text-white">
                                                <path fillRule="evenodd" d="M19.916 4.626a.75.75 0 01.208 1.04l-9 13.5a.75.75 0 01-1.154.114l-6-6a.75.75 0 011.06-1.06l5.353 5.353 8.493-12.739a.75.75 0 011.04-.208z" clipRule="evenodd" />
                                            </svg>
                                        )}
                                    </div>
                                </div>

                                {/* Document Info */}
                                <div className="flex-1 min-w-0">
                                    <div className="text-sm font-medium text-gray-200 truncate">
                                        {doc.filename}
                                    </div>
                                    <div className="text-xs text-gray-500 mt-1">
                                        {doc.page_count} page{doc.page_count !== 1 ? 's' : ''}
                                    </div>
                                </div>

                                {/* Delete Button */}
                                <button
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        handleDelete(doc.doc_id);
                                    }}
                                    className="opacity-0 group-hover:opacity-100 p-1 hover:bg-red-600/20 rounded transition-all"
                                    title="Delete document"
                                >
                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4 text-red-400">
                                        <path strokeLinecap="round" strokeLinejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                                    </svg>
                                </button>
                            </div>
                        </div>
                    ))
                )}
            </div>

            {/* Selected Count */}
            {selectedDocIds.length > 0 && (
                <div className="p-3 border-t border-gray-700 bg-gray-800">
                    <div className="text-xs text-gray-400 text-center">
                        {selectedDocIds.length} document{selectedDocIds.length !== 1 ? 's' : ''} selected
                    </div>
                </div>
            )}
        </div>
    );
}
